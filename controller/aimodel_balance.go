package controller

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
)

// AIModelBalance accepts only signed Gateway requests; it does not accept a
// browser session, API key, caller-selected owner, or additional token fields.
func AIModelBalance(c *gin.Context) {
	secret := os.Getenv("AIMODEL_GATEWAY_TOPUP_SECRET")
	userID, userErr := strconv.Atoi(os.Getenv("AIMODEL_GATEWAY_USER_ID"))
	if len(secret) < 32 || userErr != nil || userID <= 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"success": false, "code": "balance_update_unavailable"})
		return
	}
	timestamp := c.GetHeader("X-AIModel-Timestamp")
	seconds, timestampErr := strconv.ParseInt(timestamp, 10, 64)
	nonce := c.GetHeader("X-AIModel-Nonce")
	nonceBytes, nonceErr := hex.DecodeString(nonce)
	signature, signatureErr := hex.DecodeString(c.GetHeader("X-AIModel-Signature"))
	now := time.Now().Unix()
	if timestampErr != nil || strconv.FormatInt(seconds, 10) != timestamp || seconds < now-300 || seconds > now+300 || nonceErr != nil || len(nonce) != 32 || len(nonceBytes) != 16 || signatureErr != nil || len(signature) != sha256.Size {
		c.JSON(http.StatusUnauthorized, gin.H{"success": false, "code": "unauthorized"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, 4097))
	if err != nil || len(body) > 4096 {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "code": "invalid_request"})
		return
	}
	bodyHash := sha256.Sum256(body)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("aimodel:balance:v1\n" + timestamp + "\n" + nonce + "\n" + hex.EncodeToString(bodyHash[:])))
	if subtle.ConstantTimeCompare(signature, mac.Sum(nil)) != 1 {
		c.JSON(http.StatusUnauthorized, gin.H{"success": false, "code": "unauthorized"})
		return
	}
	var fields map[string]json.RawMessage
	if common.Unmarshal(body, &fields) != nil || len(fields) != 6 {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "code": "invalid_request"})
		return
	}
	for _, field := range []string{"operation_id", "token_id", "balance_quota", "expected_quota", "expected_used_quota", "actor"} {
		value, exists := fields[field]
		if !exists || string(value) == "null" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "code": "invalid_request"})
			return
		}
	}
	var req model.AIModelBalanceRequest
	if common.Unmarshal(body, &req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "code": "invalid_request"})
		return
	}
	result, err := model.SetAIModelBalance(userID, req)
	switch {
	case errors.Is(err, model.ErrAIModelBalanceInvalid):
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "code": "invalid_request"})
	case errors.Is(err, model.ErrAIModelBalanceConflict):
		c.JSON(http.StatusConflict, gin.H{"success": false, "code": "balance_conflict"})
	case errors.Is(err, model.ErrAIModelBalanceBatch):
		c.JSON(http.StatusServiceUnavailable, gin.H{"success": false, "code": "batch_update_unsupported"})
	case errors.Is(err, model.ErrAIModelBalanceTarget):
		c.JSON(http.StatusUnprocessableEntity, gin.H{"success": false, "code": "balance_target_unavailable"})
	case errors.Is(err, model.ErrAIModelBalanceCache):
		c.JSON(http.StatusServiceUnavailable, gin.H{"success": false, "code": "balance_update_uncertain", "operation_id": req.OperationID, "retryable": true})
	case err != nil:
		c.JSON(http.StatusServiceUnavailable, gin.H{"success": false, "code": "balance_update_unavailable"})
	default:
		c.JSON(http.StatusOK, gin.H{"success": true, "operation_id": req.OperationID, "token_id": req.TokenID, "actor": req.Actor, "before_quota": result.BeforeQuota, "after_quota": result.AfterQuota, "duplicate": result.Duplicate})
	}
}
