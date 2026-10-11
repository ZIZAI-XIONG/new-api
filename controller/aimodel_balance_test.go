package controller

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAIModelBalanceSignedGatewayContract(t *testing.T) {
	db := setupTokenControllerTestDB(t)
	require.NoError(t, db.AutoMigrate(&model.User{}, &model.AIModelBalanceOperation{}))
	user := model.User{Username: "balance-api-owner", Status: common.UserStatusEnabled, Quota: 500}
	require.NoError(t, db.Create(&user).Error)
	token := model.Token{UserId: user.Id, Key: "balance-api-key", Name: "balance-api", Status: common.TokenStatusEnabled, RemainQuota: 100, UsedQuota: 10, ExpiredTime: -1}
	require.NoError(t, db.Create(&token).Error)
	secret := "balance-test-secret-at-least-32-bytes"
	t.Setenv("AIMODEL_GATEWAY_TOPUP_SECRET", secret)
	t.Setenv("AIMODEL_GATEWAY_USER_ID", strconv.Itoa(user.Id))
	oldBatch := common.BatchUpdateEnabled
	common.BatchUpdateEnabled = false
	t.Cleanup(func() { common.BatchUpdateEnabled = oldBatch })
	body := fmt.Sprintf(`{"operation_id":"admin:6954caa0-e6fb-4b51-acce-f13368242220","token_id":%d,"balance_quota":200,"expected_quota":100,"expected_used_quota":10,"actor":"logto-admin"}`, token.Id)
	for _, tc := range []struct {
		name, body, domain, actor string
		age                       int64
		status                    int
		code                      string
	}{
		{name: "missing signature", body: body, status: 401, code: "unauthorized"},
		{name: "expired", body: body, domain: "aimodel:balance:v1\n", age: -301, status: 401, code: "unauthorized"},
		{name: "future signature", body: body, domain: "aimodel:balance:v1\n", age: 301, status: 401, code: "unauthorized"},
		{name: "balance signature cannot topup", body: body, domain: "aimodel:balance:v1\n", status: 401, code: ""},
		{name: "oversized request", body: strings.Repeat("x", 4097), domain: "aimodel:balance:v1\n", status: 400, code: "invalid_request"},
		{name: "batch mode rejected", body: body, domain: "aimodel:balance:v1\n", status: 503, code: "batch_update_unsupported"},
		{name: "missing configuration", body: body, domain: "aimodel:balance:v1\n", status: 503, code: "balance_update_unavailable"},
		{name: "topup signature cannot set balance", body: body, domain: "topup", status: 401, code: "unauthorized"},
		{name: "unknown owner rejected", body: strings.TrimSuffix(body, "}") + `,"user_id":1}`, domain: "aimodel:balance:v1\n", status: 400, code: "invalid_request"},
		{name: "topup fields rejected", body: strings.TrimSuffix(body, "}") + `,"quota":100}`, domain: "aimodel:balance:v1\n", status: 400, code: "invalid_request"},
		{name: "null amount rejected", body: strings.Replace(body, `"balance_quota":200`, `"balance_quota":null`, 1), domain: "aimodel:balance:v1\n", status: 400, code: "invalid_request"},
		{name: "fraction rejected", body: strings.Replace(body, `"balance_quota":200`, `"balance_quota":200.5`, 1), domain: "aimodel:balance:v1\n", status: 400, code: "invalid_request"},
		{name: "negative target rejected", body: strings.Replace(body, `"balance_quota":200`, `"balance_quota":-1`, 1), domain: "aimodel:balance:v1\n", status: 400, code: "invalid_request"},
		{name: "tampered body", body: body, domain: "aimodel:balance:v1\n", actor: "tamper", status: 401, code: "unauthorized"},
		{name: "valid", body: body, domain: "aimodel:balance:v1\n", status: 200},
		{name: "replay after consumption", body: body, domain: "aimodel:balance:v1\n", status: 200},
		{name: "different actor replay", body: strings.Replace(body, "logto-admin", "different-admin", 1), domain: "aimodel:balance:v1\n", status: 409, code: "balance_conflict"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "replay after consumption" {
				require.NoError(t, db.Model(&token).Updates(map[string]any{"remain_quota": 195, "used_quota": 15}).Error)
			}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/api/internal/aimodel/balance", bytes.NewBufferString(tc.body))
			if tc.domain != "" {
				stamp := strconv.FormatInt(time.Now().Unix()+tc.age, 10)
				nonce := "1234567890abcdef1234567890abcdef"
				hash := sha256.Sum256([]byte(tc.body))
				mac := hmac.New(sha256.New, []byte(secret))
				domain := tc.domain
				if domain == "topup" {
					domain = ""
				}
				mac.Write([]byte(domain + stamp + "\n" + nonce + "\n" + hex.EncodeToString(hash[:])))
				ctx.Request.Header.Set("X-AIModel-Timestamp", stamp)
				ctx.Request.Header.Set("X-AIModel-Nonce", nonce)
				ctx.Request.Header.Set("X-AIModel-Signature", hex.EncodeToString(mac.Sum(nil)))
				if tc.actor == "tamper" {
					ctx.Request.Body = http.NoBody
				}
			}
			if tc.name == "batch mode rejected" {
				common.BatchUpdateEnabled = true
				defer func() { common.BatchUpdateEnabled = false }()
			}
			if tc.name == "missing configuration" {
				t.Setenv("AIMODEL_GATEWAY_TOPUP_SECRET", "")
			}
			if tc.name == "balance signature cannot topup" {
				AIModelTopup(ctx)
			} else {
				AIModelBalance(ctx)
			}
			assert.Equal(t, tc.status, recorder.Code)
			var response struct {
				Success    bool   `json:"success"`
				Code       string `json:"code"`
				Duplicate  bool   `json:"duplicate"`
				AfterQuota int    `json:"after_quota"`
				Actor      string `json:"actor"`
			}
			require.NoError(t, common.Unmarshal(recorder.Body.Bytes(), &response))
			assert.Equal(t, tc.code, response.Code)
			if tc.status == 200 {
				assert.True(t, response.Success)
				assert.Equal(t, 200, response.AfterQuota)
				assert.Equal(t, "logto-admin", response.Actor)
				assert.Equal(t, tc.name == "replay after consumption", response.Duplicate)
			}
		})
	}
	require.NoError(t, db.First(&token, token.Id).Error)
	assert.Equal(t, 195, token.RemainQuota)
	assert.Equal(t, 15, token.UsedQuota)
	require.NoError(t, db.First(&user, user.Id).Error)
	assert.Equal(t, 500, user.Quota)
}
