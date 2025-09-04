package rest

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/irisnet/ibc-explorer-backend/internal/app/api/response"
)

type CacheController struct {
}

func (ctl *CacheController) Del(c *gin.Context) {
	key := c.Param("key")
	num, err := cacheService.Del(key)
	if err != nil {
		c.JSON(http.StatusOK, response.FailError(err))
		return
	}
	c.JSON(http.StatusOK, response.Success(fmt.Sprintf("del key %s, affect num: %d", key, num)))
}
