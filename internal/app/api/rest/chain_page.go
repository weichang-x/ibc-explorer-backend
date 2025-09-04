package rest

import (
	"net/http"

	"github.com/irisnet/ibc-explorer-backend/internal/app/errors"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/vo"

	"github.com/gin-gonic/gin"
	"github.com/irisnet/ibc-explorer-backend/internal/app/api/response"
)

type ChainController struct {
}

func (ctl *ChainController) List(c *gin.Context) {
	var req vo.ChainListReq
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusOK, response.FailError(errors.Wrap(err)))
		return
	}
	if req.UseCount {
		total, err := chainService.Count()
		if err != nil {
			c.JSON(http.StatusOK, response.FailError(err))
			return
		}
		c.JSON(http.StatusOK, response.Success(total))
		return
	}
	resp, err := chainService.List(&req)
	if err != nil {
		c.JSON(http.StatusOK, response.FailError(err))
		return
	}
	c.JSON(http.StatusOK, response.Success(resp))
}
