package rest

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/irisnet/ibc-explorer-backend/internal/app/api/response"
	"github.com/irisnet/ibc-explorer-backend/internal/app/errors"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/vo"
)

type ChannelController struct {
}

// List channel page
func (ctl *ChannelController) List(c *gin.Context) {
	var req vo.ChannelListReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, response.FailBadRequest(err))
		return
	}

	var res interface{}
	var err errors.Error
	if req.UseCount {
		res, err = channelService.ListCount(&req)
	} else {
		res, err = channelService.List(&req)
	}

	if err != nil {
		c.JSON(http.StatusOK, response.FailError(err))
		return
	}
	c.JSON(http.StatusOK, response.Success(res))
}
