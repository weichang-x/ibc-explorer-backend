package cache

import (
	v8 "github.com/go-redis/redis/v8"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/irisnet/ibc-explorer-backend/internal/app/repository"
	"github.com/irisnet/ibc-explorer-backend/internal/app/utils"
)

type ChainConfigCacheRepo struct {
	chainCfg repository.ChainConfigRepo
}

func (c ChainConfigCacheRepo) FindAll() ([]*entity.ChainConfig, error) {
	value, err := rc.Get(chainCfg)
	if err != nil && err == v8.Nil || len(value) == 0 {
		chains, err := c.chainCfg.FindAll()
		if err != nil {
			return nil, err
		}
		if len(chains) > 0 {
			_ = rc.Set(chainCfg, utils.MarshalJsonIgnoreErr(chains), oneMin)
			return chains, nil
		}
	}
	var data []*entity.ChainConfig
	utils.UnmarshalJsonIgnoreErr([]byte(value), &data)
	return data, nil
}
