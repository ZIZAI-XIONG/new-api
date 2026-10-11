package model

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/glebarez/sqlite"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestGrantAIModelTopup(t *testing.T) {
	require.NoError(t, DB.AutoMigrate(&AIModelTopupGrant{}))
	t.Cleanup(func() { DB.Exec("DELETE FROM ai_model_topup_grants") })
	user := User{Username: "aimodel-topup-test", Status: common.UserStatusEnabled, Quota: 200}
	require.NoError(t, DB.Create(&user).Error)
	t.Cleanup(func() { DB.Unscoped().Delete(&user) })
	token := Token{UserId: user.Id, Key: "aimodel-topup-test-token", Status: common.TokenStatusExhausted, ExpiredTime: -1, RemainQuota: -100}
	require.NoError(t, DB.Create(&token).Error)
	t.Cleanup(func() { DB.Unscoped().Delete(&token) })
	other := Token{UserId: user.Id + 1, Key: "aimodel-topup-other-token", Status: common.TokenStatusEnabled, ExpiredTime: -1}
	require.NoError(t, DB.Create(&other).Error)
	t.Cleanup(func() { DB.Unscoped().Delete(&other) })

	result, err := GrantAIModelTopup("stripe:cs_test_001", user.Id, token.Id, 50)
	require.NoError(t, err)
	require.False(t, result.Duplicate)
	require.Equal(t, -100, result.BeforeQuota)
	require.Equal(t, -50, result.AfterQuota)
	require.NoError(t, DB.First(&token, token.Id).Error)
	require.Equal(t, -50, token.RemainQuota)
	require.Equal(t, common.TokenStatusExhausted, token.Status)
	require.NoError(t, DB.First(&user, user.Id).Error)
	require.Equal(t, 200, user.Quota)

	result, err = GrantAIModelTopup("stripe:cs_test_001", user.Id, token.Id, 50)
	require.NoError(t, err)
	require.True(t, result.Duplicate)
	require.Equal(t, -100, result.BeforeQuota)
	require.Equal(t, -50, result.AfterQuota)
	_, err = GrantAIModelTopup("stripe:cs_test_001", user.Id, token.Id, 51)
	require.ErrorIs(t, err, ErrAIModelTopupConflict)
	_, err = GrantAIModelTopup("stripe:cs_test_002", user.Id, other.Id, 50)
	require.ErrorIs(t, err, ErrAIModelTopupTarget)
	result, err = GrantAIModelTopup("stripe:cs_test_002", user.Id, token.Id, 100)
	require.NoError(t, err) // Failed ownership check rolled back its operation ID.
	require.Equal(t, -50, result.BeforeQuota)
	require.Equal(t, 50, result.AfterQuota)
	require.NoError(t, DB.First(&token, token.Id).Error)
	require.Equal(t, 50, token.RemainQuota)
	require.Equal(t, common.TokenStatusEnabled, token.Status)
	require.NoError(t, DB.First(&user, user.Id).Error)
	require.Equal(t, 200, user.Quota)

	_, err = GrantAIModelTopup("bad", user.Id, token.Id, 10)
	require.ErrorIs(t, err, ErrAIModelTopupInvalid)
	_, err = GrantAIModelTopup("stripe:cs_test_003", user.Id, token.Id, common.MaxWalletQuota)
	require.True(t, errors.Is(err, ErrAIModelTopupTarget)) // Atomic overflow guard.
	require.NoError(t, DB.First(&user, user.Id).Error)
	require.Equal(t, 200, user.Quota)
	var count int64
	require.NoError(t, DB.Model(&AIModelTopupGrant{}).Count(&count).Error)
	require.EqualValues(t, 2, count)
}

func TestGrantAIModelTopupRetryRepairsCache(t *testing.T) {
	require.NoError(t, DB.AutoMigrate(&AIModelTopupGrant{}))
	user := User{Username: "aimodel-cache-repair", Status: common.UserStatusEnabled, AuthVersion: 1}
	require.NoError(t, DB.Create(&user).Error)
	t.Cleanup(func() { DB.Unscoped().Delete(&user) })
	token := Token{UserId: user.Id, Key: "aimodel-cache-repair-token", Status: common.TokenStatusEnabled, ExpiredTime: -1, RemainQuota: 5}
	require.NoError(t, DB.Create(&token).Error)
	t.Cleanup(func() { DB.Unscoped().Delete(&token) })
	t.Cleanup(func() { DB.Where("operation_id = ?", "stripe:cs_cache_repair").Delete(&AIModelTopupGrant{}) })
	_, err := GrantAIModelTopup("stripe:cs_cache_repair", user.Id, token.Id, 20)
	require.NoError(t, err)

	useUserCacheMiniRedis(t)
	// The shared operating pool cache must remain unchanged by a personal top-up.
	user.Quota = 0
	require.NoError(t, populateUserCache(user))
	// Simulate a stale personal budget snapshot after the durable grant.
	_, err = cacheInitToken(token)
	require.NoError(t, err)
	healthyClient := common.RDB
	brokenClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, ReadTimeout: 100 * time.Millisecond, WriteTimeout: 100 * time.Millisecond, MaxRetries: 0})
	common.RDB = brokenClient
	_, err = GrantAIModelTopup("stripe:cs_cache_repair", user.Id, token.Id, 20)
	require.ErrorIs(t, err, ErrAIModelTopupCache)
	common.RDB = healthyClient
	require.NoError(t, brokenClient.Close())
	result, err := GrantAIModelTopup("stripe:cs_cache_repair", user.Id, token.Id, 20)
	require.NoError(t, err)
	assert.True(t, result.Duplicate)
	assert.Equal(t, 5, result.BeforeQuota)
	assert.Equal(t, 25, result.AfterQuota)
	quota, err := GetUserQuota(user.Id, false)
	require.NoError(t, err)
	assert.Equal(t, 0, quota)
	cached, err := GetTokenByKey(token.Key, false)
	require.NoError(t, err)
	assert.Equal(t, 25, cached.RemainQuota)
	require.NoError(t, DB.First(&token, token.Id).Error)
	assert.Equal(t, 25, token.RemainQuota)
}

// Run this test against disposable real databases via TEST_MYSQL_DSN and
// TEST_POSTGRES_DSN. It exercises the unique operation key, row locks, and
// migration repeatability on their actual dialects.
func TestGrantAIModelTopupDialects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		dsn    string
		open   func(string) gorm.Dialector
		dbType common.DatabaseType
	}{
		{"sqlite", "local", func(dsn string) gorm.Dialector { return sqlite.Open(dsn) }, common.DatabaseTypeSQLite},
		{"mysql", os.Getenv("TEST_MYSQL_DSN"), func(dsn string) gorm.Dialector { return mysql.Open(dsn) }, common.DatabaseTypeMySQL},
		{"postgres", os.Getenv("TEST_POSTGRES_DSN"), func(dsn string) gorm.Dialector { return postgres.Open(dsn) }, common.DatabaseTypePostgreSQL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.TrimSpace(tc.dsn) == "" {
				t.Skip("test database DSN is not configured")
			}
			dsn := tc.dsn
			if tc.dbType == common.DatabaseTypeSQLite {
				dsn = filepath.Join(t.TempDir(), "topup.db") + "?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_txlock=immediate"
			}
			db, err := gorm.Open(tc.open(dsn), &gorm.Config{})
			require.NoError(t, err)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			if tc.dbType == common.DatabaseTypeSQLite {
				sqlDB.SetMaxOpenConns(1)
			}
			t.Cleanup(func() { assert.NoError(t, sqlDB.Close()) })
			previousDB := DB
			DB = db
			common.SetDatabaseTypes(tc.dbType, tc.dbType)
			initCol()
			t.Cleanup(func() {
				DB = previousDB
				common.SetDatabaseTypes(common.DatabaseTypeSQLite, common.DatabaseTypeSQLite)
				initCol()
			})
			// Simulate the prior release's schema, then upgrade and restart.
			require.NoError(t, DB.AutoMigrate(&User{}, &Token{}, &AIModelTopupGrant{}))
			user := User{Username: "aimodel-topup-matrix", Status: common.UserStatusEnabled, Quota: 0}
			require.NoError(t, DB.Create(&user).Error)
			t.Cleanup(func() { DB.Unscoped().Delete(&user) })
			token := Token{UserId: user.Id, Key: "aimodel-topup-matrix-token", Status: common.TokenStatusExhausted, ExpiredTime: -1, RemainQuota: -10}
			require.NoError(t, DB.Create(&token).Error)
			t.Cleanup(func() { DB.Unscoped().Delete(&token) })
			priorGrant := AIModelTopupGrant{OperationID: "stripe:cs_prior_release", AttemptID: "74ff8e3a-b415-4439-ac5a-555b2210917e", UserID: user.Id, TokenID: token.Id, Quota: 20, BeforeQuota: -30, AfterQuota: -10}
			require.NoError(t, DB.Create(&priorGrant).Error)
			require.NoError(t, DB.First(&priorGrant, priorGrant.ID).Error)
			t.Cleanup(func() { DB.Delete(&priorGrant) })
			for range 2 {
				require.NoError(t, migrateDB())
			}
			require.NoError(t, DB.First(&user, user.Id).Error)
			require.NoError(t, DB.First(&token, token.Id).Error)
			assert.Equal(t, -10, token.RemainQuota)
			var restoredGrant AIModelTopupGrant
			require.NoError(t, DB.First(&restoredGrant, priorGrant.ID).Error)
			assert.Equal(t, priorGrant, restoredGrant)
			t.Cleanup(func() {
				DB.Where("operation_id IN ?", []string{"stripe:cs_matrix_001", "stripe:cs_matrix_002"}).Delete(&AIModelTopupGrant{})
			})
			result, err := GrantAIModelTopup("stripe:cs_matrix_001", user.Id, token.Id, 20)
			require.NoError(t, err)
			assert.Equal(t, -10, result.BeforeQuota)
			assert.Equal(t, 10, result.AfterQuota)
			result, err = GrantAIModelTopup("stripe:cs_matrix_001", user.Id, token.Id, 20)
			require.NoError(t, err)
			assert.True(t, result.Duplicate)
			assert.Equal(t, -10, result.BeforeQuota)
			assert.Equal(t, 10, result.AfterQuota)
			var starts sync.WaitGroup
			starts.Add(1)
			results := make(chan AIModelTopupResult, 2)
			errors := make(chan error, 2)
			for range 2 {
				go func() {
					starts.Wait()
					r, err := GrantAIModelTopup("stripe:cs_matrix_002", user.Id, token.Id, 5)
					results <- r
					errors <- err
				}()
			}
			starts.Done()
			first, second := <-results, <-results
			require.NoError(t, <-errors)
			require.NoError(t, <-errors)
			assert.NotEqual(t, first.Duplicate, second.Duplicate)
			assert.Equal(t, 10, first.BeforeQuota)
			assert.Equal(t, 15, first.AfterQuota)
			assert.Equal(t, 10, second.BeforeQuota)
			assert.Equal(t, 15, second.AfterQuota)
			require.NoError(t, DB.First(&token, token.Id).Error)
			assert.Equal(t, 15, token.RemainQuota)
			require.NoError(t, DB.First(&user, user.Id).Error)
			assert.Equal(t, 0, user.Quota)
			// Verify the new operation's upgrade, uniqueness, CAS and retry on each real dialect.
			require.True(t, DB.Migrator().HasIndex(&AIModelTopupGrant{}, "idx_ai_model_topup_grants_operation_id"))
			require.True(t, DB.Migrator().HasIndex(&AIModelBalanceOperation{}, "idx_ai_model_balance_operations_operation_id"))
			ip := "127.0.0.1"
			require.NoError(t, DB.Model(&token).Updates(map[string]any{"name": "matrix-principal", "model_limits_enabled": true, "model_limits": "openai/model", "allow_ips": ip, "group": "auto", "cross_group_retry": true, "auto_groups": `["default"]`}).Error)
			require.NoError(t, DB.First(&token, token.Id).Error)
			original := token
			balance := AIModelBalanceRequest{OperationID: "admin:ae7c1fe8-b8df-48de-becb-000000000001", TokenID: token.Id, Actor: "matrix-admin", BalanceQuota: 100, ExpectedQuota: token.RemainQuota, ExpectedUsedQuota: token.UsedQuota}
			t.Cleanup(func() { DB.Where("actor = ?", "matrix-admin").Delete(&AIModelBalanceOperation{}) })
			_, err = SetAIModelBalance(user.Id, balance)
			require.NoError(t, err)
			require.NoError(t, decreaseTokenQuota(token.Id, 5))
			result, err = SetAIModelBalance(user.Id, balance)
			require.NoError(t, err)
			assert.True(t, result.Duplicate)
			require.NoError(t, DB.First(&token, token.Id).Error)
			original.RemainQuota = 95
			original.UsedQuota += 5
			original.AccessedTime = token.AccessedTime
			assert.Equal(t, original, token)
			// Two administrators with the same snapshot: exactly one write wins.
			outcomes := make(chan error, 2)
			start := make(chan struct{})
			for i := range 2 {
				go func() {
					<-start
					request := balance
					request.OperationID = fmt.Sprintf("admin:ae7c1fe8-b8df-48de-becb-%012d", i+2)
					request.ExpectedQuota = 95
					request.ExpectedUsedQuota = token.UsedQuota
					request.BalanceQuota = 200 + i
					_, err := SetAIModelBalance(user.Id, request)
					outcomes <- err
				}()
			}
			close(start)
			firstErr, secondErr := <-outcomes, <-outcomes
			if firstErr == nil {
				require.ErrorIs(t, secondErr, ErrAIModelBalanceConflict)
			} else {
				require.ErrorIs(t, firstErr, ErrAIModelBalanceConflict)
				require.NoError(t, secondErr)
			}
			var operations int64
			require.NoError(t, DB.Model(&AIModelBalanceOperation{}).Where("actor = ?", "matrix-admin").Count(&operations).Error)
			assert.EqualValues(t, 2, operations)
			require.NoError(t, DB.First(&user, user.Id).Error)
			assert.Equal(t, 0, user.Quota)
		})
	}
}

func TestSetAIModelBalance(t *testing.T) {
	require.NoError(t, DB.AutoMigrate(&AIModelBalanceOperation{}))
	user := User{Username: "balance-admin-test", Status: common.UserStatusEnabled, Quota: 500}
	require.NoError(t, DB.Create(&user).Error)
	t.Cleanup(func() {
		DB.Unscoped().Delete(&user)
		DB.Where("actor = ?", "admin-logto-test").Delete(&AIModelBalanceOperation{})
	})
	ip := "127.0.0.1"
	token := Token{UserId: user.Id, Key: "balance-test-token", Name: "principal", Status: common.TokenStatusExhausted, RemainQuota: -10, UsedQuota: 20, ExpiredTime: -1, ModelLimitsEnabled: true, ModelLimits: "openai/model", AllowIps: &ip, Group: "auto", CrossGroupRetry: true, AutoGroups: `["default"]`}
	require.NoError(t, DB.Create(&token).Error)
	t.Cleanup(func() { DB.Unscoped().Delete(&token) })
	original := token
	req := AIModelBalanceRequest{OperationID: "admin:dd9983d7-8c34-4f07-a2e2-137f959e9fb4", TokenID: token.Id, BalanceQuota: 100, ExpectedQuota: -10, ExpectedUsedQuota: 20, Actor: "admin-logto-test"}
	stale := req
	stale.ExpectedUsedQuota = 19
	_, err := SetAIModelBalance(user.Id, stale)
	require.ErrorIs(t, err, ErrAIModelBalanceConflict)
	_, err = SetAIModelBalance(user.Id+1, req)
	require.ErrorIs(t, err, ErrAIModelBalanceTarget)
	oldBatch := common.BatchUpdateEnabled
	t.Cleanup(func() { common.BatchUpdateEnabled = oldBatch })
	common.BatchUpdateEnabled = true
	_, err = SetAIModelBalance(user.Id, req)
	require.ErrorIs(t, err, ErrAIModelBalanceBatch)
	common.BatchUpdateEnabled = oldBatch
	result, err := SetAIModelBalance(user.Id, req)
	require.NoError(t, err)
	assert.Equal(t, AIModelTopupResult{BeforeQuota: -10, AfterQuota: 100}, result)
	require.NoError(t, DB.First(&token, token.Id).Error)
	assert.Equal(t, 100, token.RemainQuota)
	assert.Equal(t, 20, token.UsedQuota)
	assert.Equal(t, common.TokenStatusEnabled, token.Status)
	original.RemainQuota = 100
	original.Status = common.TokenStatusEnabled
	assert.Equal(t, original, token)
	require.NoError(t, DB.First(&user, user.Id).Error)
	assert.Equal(t, 500, user.Quota)
	// Consumption after a successful write must survive retries; batch mode blocks repair.
	require.NoError(t, decreaseTokenQuota(token.Id, 5))
	common.BatchUpdateEnabled = true
	_, err = SetAIModelBalance(user.Id, req)
	require.ErrorIs(t, err, ErrAIModelBalanceBatch)
	common.BatchUpdateEnabled = oldBatch
	result, err = SetAIModelBalance(user.Id, req)
	require.NoError(t, err)
	assert.True(t, result.Duplicate)
	assert.Equal(t, 100, result.AfterQuota)
	require.NoError(t, DB.First(&token, token.Id).Error)
	assert.Equal(t, 95, token.RemainQuota)
	assert.Equal(t, 25, token.UsedQuota)
	changed := req
	changed.Actor = "another-admin"
	_, err = SetAIModelBalance(user.Id, changed)
	require.ErrorIs(t, err, ErrAIModelBalanceConflict)
	changed = req
	changed.BalanceQuota = 101
	_, err = SetAIModelBalance(user.Id, changed)
	require.ErrorIs(t, err, ErrAIModelBalanceConflict)
	// Existing token restrictions remain effective after editing its budget.
	for i, status := range []int{common.TokenStatusDisabled, common.TokenStatusExpired, common.TokenStatusExhausted} {
		require.NoError(t, DB.Model(&token).Updates(map[string]any{"status": status, "expired_time": 1}).Error)
		next := req
		next.OperationID = fmt.Sprintf("admin:dd9983d7-8c34-4f07-a2e2-%012d", i+1)
		next.ExpectedQuota = 95
		next.ExpectedUsedQuota = 25
		next.BalanceQuota = 95
		_, err = SetAIModelBalance(user.Id, next)
		require.NoError(t, err)
		require.NoError(t, DB.First(&token, token.Id).Error)
		assert.Equal(t, status, token.Status)
	}
	useUserCacheMiniRedis(t)
	healthy := common.RDB
	t.Cleanup(func() { common.RDB = healthy })
	broken := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, ReadTimeout: 100 * time.Millisecond, WriteTimeout: 100 * time.Millisecond, MaxRetries: 0})
	defer broken.Close()
	next := req
	next.OperationID = "admin:dd9983d7-8c34-4f07-a2e2-137f959e9fb5"
	next.ExpectedQuota = 95
	next.ExpectedUsedQuota = 25
	next.BalanceQuota = 120
	common.RDB = broken
	_, err = SetAIModelBalance(user.Id, next)
	require.ErrorIs(t, err, ErrAIModelBalanceCache)
	require.NoError(t, DB.First(&token, token.Id).Error)
	assert.Equal(t, 95, token.RemainQuota)
	common.RDB = healthy
	// Force Redis failure after the quota SQL update but before post-commit cache repair.
	callback := "test:balance-cache-after-write"
	require.NoError(t, DB.Callback().Update().After("gorm:update").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "tokens" {
			common.RDB = broken
		}
	}))
	t.Cleanup(func() { DB.Callback().Update().Remove(callback) })
	_, err = SetAIModelBalance(user.Id, next)
	require.ErrorIs(t, err, ErrAIModelBalanceCache)
	require.NoError(t, DB.Callback().Update().Remove(callback))
	common.RDB = healthy
	require.NoError(t, DB.First(&token, token.Id).Error)
	assert.Equal(t, 120, token.RemainQuota)
	require.NoError(t, decreaseTokenQuota(token.Id, 5))
	result, err = SetAIModelBalance(user.Id, next)
	require.NoError(t, err)
	assert.True(t, result.Duplicate)
	assert.Equal(t, 120, result.AfterQuota)
	cached, err := GetTokenByKey(token.Key, false)
	require.NoError(t, err)
	assert.Equal(t, 115, cached.RemainQuota)
}
