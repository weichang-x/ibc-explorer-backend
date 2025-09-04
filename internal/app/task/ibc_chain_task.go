package task

import (
	"context"
	"time"

	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/sirupsen/logrus"
)

type IbcChainCronTask struct {
}

func (t *IbcChainCronTask) Name() string {
	return "ibc_chain_task"
}
func (t *IbcChainCronTask) Cron() int {
	if taskConf.CronTimeChainTask > 0 {
		return taskConf.CronTimeChainTask
	}
	return EveryMinute
}
func (t *IbcChainCronTask) Run() int {
	chainCfgs, err := chainConfigRepo.FindAll()
	if err != nil {
		logrus.Errorf("task %s run error, %s", t.Name(), err.Error())
		return -1
	}
	var chains []entity.IBCChain
	for i := range chainCfgs {
		conntectedChains := len(chainCfgs[i].IbcInfo)
		channels := 0
		for _, val := range chainCfgs[i].IbcInfo {
			channels += len(val.Paths)
		}
		data := createChainData(chainCfgs[i].ChainName, channels, conntectedChains)
		chains = append(chains, data)
	}

	dbChains, _ := chainCache.FindAll()
	ibcChainMap := make(map[string]struct{}, len(dbChains))
	for i := range dbChains {
		ibcChainMap[dbChains[i].Chain] = struct{}{}
	}

	bulk := chainRepo.Bulk()
	for i := range chains {
		_, ok := ibcChainMap[chains[i].Chain]
		chainRepo.InserOrUpdate(bulk, chains[i], ok)
	}
	bulk.SetOrdered(false)
	if _, err := bulk.Run(context.Background()); err != nil {
		logrus.Errorf("task %s bulk ibc_chain inser or update error, %s", t.Name(), err.Error())
		return -1
	}
	return 1

}
func createChainData(chain string, channels int, conntectedChains int) entity.IBCChain {
	return entity.IBCChain{
		Chain:           chain,
		Channels:        int64(channels),
		ConnectedChains: int64(conntectedChains),
		CreateAt:        time.Now().Unix(),
		UpdateAt:        time.Now().Unix(),
	}
}
func (t *IbcChainCronTask) ExpireTime() time.Duration {
	return 1*time.Minute - 1*time.Second
}
