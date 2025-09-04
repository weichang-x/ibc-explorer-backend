package rest

import (
	"fmt"
	"net/http"

	"github.com/irisnet/ibc-explorer-backend/internal/app/errors"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/vo"

	"github.com/gin-gonic/gin"
	"github.com/irisnet/ibc-explorer-backend/internal/app/api/response"
)

type OverviewController struct {
}

func (ctl *OverviewController) MarketHeatmap(c *gin.Context) {
	resp, err := overviewService.MarketHeatmap()
	if err != nil {
		c.JSON(http.StatusOK, response.FailError(err))
		return
	}
	c.JSON(http.StatusOK, response.Success(resp))
}

func (ctl *OverviewController) ChainVolume(c *gin.Context) {
	var req vo.ChainVolumeReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, response.FailBadRequest(err))
		return
	}

	var res interface{}
	var err errors.Error

	res, err = overviewService.ChainVolume(&req)

	if err != nil {
		c.JSON(http.StatusOK, response.FailError(err))
		return
	}
	c.JSON(http.StatusOK, response.Success(res))
}

func (ctl *OverviewController) ChainVolumeTrend(c *gin.Context) {
	var req vo.ChainVolumeTrendReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, response.FailBadRequest(err))
		return
	}

	var res interface{}
	var err errors.Error

	res, err = overviewService.ChainVolumeTrend(&req)

	if err != nil {
		c.JSON(http.StatusOK, response.FailError(err))
		return
	}
	c.JSON(http.StatusOK, response.Success(res))
}

func (ctl *OverviewController) TokenDistribution(c *gin.Context) {

	var req vo.TokenDistributionReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, response.FailBadRequest(err))
		return
	}

	if req.BaseDenom == "" || req.BaseDenomChain == "" {
		c.JSON(http.StatusOK, response.FailBadRequest(fmt.Errorf("invalid parameters")))
		return
	}

	resp, err := overviewService.TokenDistribution(&req)
	if err != nil {
		c.JSON(http.StatusOK, response.FailError(err))
		return
	}
	c.JSON(http.StatusOK, response.Success(resp))
}
