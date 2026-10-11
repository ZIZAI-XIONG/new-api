package model

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/QuantumNous/new-api/common"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrAIModelBalanceInvalid  = errors.New("invalid balance request")
	ErrAIModelBalanceConflict = errors.New("balance conflict")
	ErrAIModelBalanceTarget   = errors.New("balance target unavailable")
	ErrAIModelBalanceBatch    = errors.New("batch update unsupported")
	ErrAIModelBalanceCache    = errors.New("balance cache synchronization uncertain")
)

// AIModelBalanceOperation is an immutable audit and retry record, not a wallet.
type AIModelBalanceOperation struct {
	ID                uint   `gorm:"primaryKey"`
	OperationID       string `gorm:"type:varchar(128);uniqueIndex;not null"`
	AttemptID         string `gorm:"type:char(36);not null"`
	Actor             string `gorm:"type:varchar(128);not null"`
	UserID            int    `gorm:"not null"`
	TokenID           int    `gorm:"not null"`
	ExpectedQuota     int    `gorm:"not null"`
	ExpectedUsedQuota int    `gorm:"not null"`
	BalanceQuota      int    `gorm:"not null"`
	BeforeQuota       int    `gorm:"not null"`
	AfterQuota        int    `gorm:"not null"`
	CreatedAt         time.Time
}

type AIModelBalanceRequest struct {
	OperationID       string `json:"operation_id"`
	TokenID           int    `json:"token_id"`
	BalanceQuota      int    `json:"balance_quota"`
	ExpectedQuota     int    `json:"expected_quota"`
	ExpectedUsedQuota int    `json:"expected_used_quota"`
	Actor             string `json:"actor"`
}

// SetAIModelBalance atomically compares a personal token's balance and usage,
// writes only the balance, and records the administrator's operation. A retry
// never reapplies the balance, even if consumption occurred after the first call.
func SetAIModelBalance(userID int, req AIModelBalanceRequest) (result AIModelTopupResult, err error) {
	operationUUID, uuidErr := uuid.Parse(strings.TrimPrefix(req.OperationID, "admin:"))
	if uuidErr != nil || req.OperationID != "admin:"+operationUUID.String() || userID <= 0 || req.TokenID <= 0 || len(req.Actor) == 0 || len(req.Actor) > 128 || strings.IndexFunc(req.Actor, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 || req.BalanceQuota < 0 || req.BalanceQuota > common.MaxWalletQuota || req.ExpectedQuota < -common.MaxWalletQuota || req.ExpectedQuota > common.MaxWalletQuota || req.ExpectedUsedQuota < 0 || req.ExpectedUsedQuota > common.MaxWalletQuota {
		return result, ErrAIModelBalanceInvalid
	}
	// Batch-mode Redis balances can be ahead of the DB. Even an idempotent
	// retry must not invalidate those snapshots until queued deltas are flushed.
	if common.BatchUpdateEnabled {
		return result, ErrAIModelBalanceBatch
	}
	err = DB.Transaction(func(tx *gorm.DB) error {
		op := AIModelBalanceOperation{OperationID: req.OperationID, AttemptID: uuid.NewString(), Actor: req.Actor, UserID: userID, TokenID: req.TokenID, ExpectedQuota: req.ExpectedQuota, ExpectedUsedQuota: req.ExpectedUsedQuota, BalanceQuota: req.BalanceQuota, BeforeQuota: req.ExpectedQuota, AfterQuota: req.BalanceQuota}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "operation_id"}}, DoNothing: true}).Create(&op).Error; err != nil {
			return err
		}
		var saved AIModelBalanceOperation
		if err := lockForUpdate(tx).Where("operation_id = ?", req.OperationID).First(&saved).Error; err != nil {
			return err
		}
		if saved.AttemptID != op.AttemptID {
			if saved.Actor != req.Actor || saved.UserID != userID || saved.TokenID != req.TokenID || saved.ExpectedQuota != req.ExpectedQuota || saved.ExpectedUsedQuota != req.ExpectedUsedQuota || saved.BalanceQuota != req.BalanceQuota {
				return ErrAIModelBalanceConflict
			}
			result = AIModelTopupResult{Duplicate: true, BeforeQuota: saved.BeforeQuota, AfterQuota: saved.AfterQuota}
			return nil
		}
		// Pending in-memory quota deltas cannot participate in the database CAS.
		if common.BatchUpdateEnabled {
			return ErrAIModelBalanceBatch
		}
		var user User
		if err := tx.Select("id", "status").Where("id = ?", userID).First(&user).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrAIModelBalanceTarget
			}
			return err
		}
		if user.Status != common.UserStatusEnabled {
			return ErrAIModelBalanceTarget
		}
		var token Token
		if err := lockForUpdate(tx).Where("id = ? AND user_id = ?", req.TokenID, userID).First(&token).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrAIModelBalanceTarget
			}
			return err
		}
		if token.UnlimitedQuota {
			return ErrAIModelBalanceTarget
		}
		if token.RemainQuota != req.ExpectedQuota || token.UsedQuota != req.ExpectedUsedQuota {
			return ErrAIModelBalanceConflict
		}
		if err := invalidateTokenCacheForMutation(token.Key); err != nil {
			return fmt.Errorf("%w: %v", ErrAIModelBalanceCache, err)
		}
		if req.BalanceQuota != req.ExpectedQuota {
			update := tx.Model(&Token{}).Where("id = ? AND user_id = ? AND remain_quota = ? AND used_quota = ?", req.TokenID, userID, req.ExpectedQuota, req.ExpectedUsedQuota).Update("remain_quota", req.BalanceQuota)
			if update.Error != nil {
				return update.Error
			}
			if update.RowsAffected != 1 {
				return ErrAIModelBalanceConflict
			}
		}
		if err := tx.Model(&Token{}).Where("id = ? AND status = ? AND remain_quota > 0 AND (expired_time = -1 OR expired_time > ?)", req.TokenID, common.TokenStatusExhausted, common.GetTimestamp()).Update("status", common.TokenStatusEnabled).Error; err != nil {
			return err
		}
		result = AIModelTopupResult{BeforeQuota: req.ExpectedQuota, AfterQuota: req.BalanceQuota}
		return nil
	})
	if err != nil {
		return result, err
	}
	// Repair only the cache; never rewrite a committed operation's balance.
	if common.BatchUpdateEnabled {
		return result, ErrAIModelBalanceBatch
	}
	if common.RedisEnabled {
		var token Token
		if err := DB.Unscoped().Select("key").Where("id = ? AND user_id = ?", req.TokenID, userID).First(&token).Error; err != nil {
			return result, fmt.Errorf("%w: %v", ErrAIModelBalanceCache, err)
		}
		if err := invalidateTokenCacheForMutation(token.Key); err != nil {
			return result, fmt.Errorf("%w: %v", ErrAIModelBalanceCache, err)
		}
	}
	return result, nil
}
