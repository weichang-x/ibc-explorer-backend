package rest

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/irisnet/ibc-explorer-backend/internal/app/api/response"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/vo"
)

type IbcTransferController struct {
}

func (ctl *IbcTransferController) TransferTxs(c *gin.Context) {
	var req vo.TranaferTxsReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, response.FailBadRequest(err))
		return
	}
	if req.UseCount {
		count, err := transferService.TransferTxsCount(&req)
		if err != nil {
			c.JSON(http.StatusOK, response.FailError(err))
			return
		}
		c.JSON(http.StatusOK, response.Success(count))
		return
	}
	resp, err := transferService.TransferTxs(&req)
	if err != nil {
		c.JSON(http.StatusOK, response.FailError(err))
		return
	}
	c.JSON(http.StatusOK, response.Success(resp))
}

func (ctl *IbcTransferController) TransferTxsVolume(c *gin.Context) {
	var req vo.TranaferTxsValueReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, response.FailBadRequest(err))
		return
	}

	resp, err := transferService.TransferTxsVolume(&req)
	if err != nil {
		c.JSON(http.StatusOK, response.FailError(err))
		return
	}
	c.JSON(http.StatusOK, response.Success(resp))
	return
}

func (ctl *IbcTransferController) TransferTxDetailNew(c *gin.Context) {
	hash := c.Param("hash")
	resp, err := transferService.TransferTxDetailNew(hash)
	if err != nil {
		c.JSON(http.StatusOK, response.FailError(err))
		return
	}
	c.JSON(http.StatusOK, response.Success(resp))
}

func (ctl *IbcTransferController) TraceSource(c *gin.Context) {
	hash := c.Param("hash")
	var req vo.TraceSourceReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, response.FailBadRequest(err))
		return
	}
	resp, err := transferService.TraceSource(hash, &req)
	if err != nil {
		c.JSON(http.StatusOK, response.FailError(err))
		return
	}
	c.JSON(http.StatusOK, response.Success(resp))
}

func (ctl *IbcTransferController) SearchCondition(c *gin.Context) {
	resp, err := transferService.SearchCondition()
	if err != nil {
		c.JSON(http.StatusOK, response.FailError(err))
		return
	}
	c.JSON(http.StatusOK, response.Success(resp))
}
