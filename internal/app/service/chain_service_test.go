package service

import (
	"context"

	"testing"

	conf "github.com/irisnet/ibc-explorer-backend/internal/app/config"
	"github.com/irisnet/ibc-explorer-backend/internal/app/global"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/vo"
	"github.com/irisnet/ibc-explorer-backend/internal/app/repository"
	"github.com/irisnet/ibc-explorer-backend/internal/app/repository/cache"
)

func TestMain(m *testing.M) {
	cache.InitRedisClient(conf.Redis{
		Addrs:    "127.0.0.1:6379",
		User:     "",
		Password: "",
		Mode:     "single",
		Db:       0,
	})
	repository.InitMgo(conf.Mongo{
		Url:      "mongodb://username:password@host:port/?authSource=database",
		Database: "ibc_explorer",
	}, context.Background())
	global.Config = &conf.Config{
		App: conf.App{
			MaxPageSize: 100,
		},
	}
	m.Run()
}

func TestChainService_List(t *testing.T) {
	resp, err := new(ChainService).List(&vo.ChainListReq{})
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(resp)
}
