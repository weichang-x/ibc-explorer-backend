package task

import (
	"sync"
	"time"

	"github.com/irisnet/ibc-explorer-backend/internal/app/pkg/distributiontask"

	"github.com/irisnet/ibc-explorer-backend/internal/app/global"
	"github.com/irisnet/ibc-explorer-backend/internal/app/utils"
	"github.com/sirupsen/logrus"
)

type IbcTxRelateHistoryTask struct {
}

func (t *IbcTxRelateHistoryTask) BeforeHook() error {
	return nil
}

var _ distributiontask.CronTask = new(IbcTxRelateHistoryTask)

//var relateHistoryCoordinator *stringQueueCoordinator

func (t *IbcTxRelateHistoryTask) Name() string {
	return "ibc_tx_relate_history_task"
}

func (t *IbcTxRelateHistoryTask) Cron() string {
	if taskConf.CronIbcTxRelateHistoryTask != "" {
		return taskConf.CronIbcTxRelateHistoryTask
	}
	return "0 0 7 * * ?"
}

func (t *IbcTxRelateHistoryTask) workerNum() int {
	if global.Config.Task.IbcTxRelateWorkerNum > 0 {
		return global.Config.Task.IbcTxRelateWorkerNum
	}
	return ibcTxRelateTaskWorkerNum
}

func (t *IbcTxRelateHistoryTask) Run() {
	chainMap, err := getAllChainMap()
	if err != nil {
		logrus.Errorf("task %s getAllChainMap error, %v", t.Name(), err)
		return
	}

	// init coordinator
	chainQueue := new(utils.QueueString)
	chainQueueHistory := new(utils.QueueString)
	for _, v := range chainMap {
		chainQueue.Push(v.ChainName)
		chainQueueHistory.Push(v.ChainName)
	}
	// handle history ibc tx created on latest three months
	now := time.Now()
	beginTime := now.Add(-3 * 30 * 24 * time.Hour).Unix()
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func(wn string) {
		newIbcTxRelateWorker(t.Name(), wn, ibcTxTargetLatest, chainMap).exec(beginTime, chainQueue)
		waitGroup.Done()
	}("ex_ibc_tx_latest")

	go func(wn string) {
		newIbcTxRelateWorker(t.Name(), wn, ibcTxTargetHistory, chainMap).exec(beginTime, chainQueueHistory)
		waitGroup.Done()
	}("ex_ibc_tx")

	waitGroup.Wait()
	logrus.Infof("task %s end,time use: %d(s)", t.Name(), time.Now().Unix()-now.Unix())
	return
}
