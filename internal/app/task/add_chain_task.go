package task

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/irisnet/ibc-explorer-backend/internal/app/constant"
	"github.com/irisnet/ibc-explorer-backend/internal/app/global"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/irisnet/ibc-explorer-backend/internal/app/pkg/ibctool"
	"github.com/sirupsen/logrus"
)

type AddChainTask struct {
}

var _ OneOffTask = new(AddChainTask)

func (t *AddChainTask) Name() string {
	return "add_chain_task"
}

func (t *AddChainTask) Switch() bool {
	return global.Config.Task.SwitchAddChainTask
}

func (t *AddChainTask) Run() int {
	chainsStr := global.Config.ChainConfig.NewChains
	newChains := strings.Split(chainsStr, ",")
	if len(newChains) == 0 {
		logrus.Errorf("task %s don't have new chains", t.Name())
		return 1
	}

	return t.handle(newChains)
}

func (t *AddChainTask) RunWithParam(chainsStr string) int {
	newChains := strings.Split(chainsStr, ",")
	if len(newChains) == 0 {
		logrus.Errorf("task %s don't have new chains", t.Name())
		return 1
	}

	return t.handle(newChains)
}

func (t *AddChainTask) handle(newChains []string) int {
	chainMap, err := getAllChainMap()
	if err != nil {
		logrus.Errorf("task %s getAllChainMap error, %v", t.Name(), err)
		return -1
	}

	createAt := int64(0)
	for {
		denomList, err := denomRepo.FindAll(createAt, constant.DefaultLimit)
		if err != nil {
			logrus.Errorf("task %s denomRepo.FindAll error, %v", t.Name(), err)
			return -1
		}

		// update denom
		t.updateDenom(denomList, chainMap)

		createAt = denomList[len(denomList)-1].CreateAt
		if len(denomList) <= constant.DefaultLimit {
			break
		}
	}

	// update ibc tx
	for _, chain := range newChains {
		chainConfig, ok := chainMap[chain]
		if !ok {
			logrus.Warningf("task %s %s dont't have chain config", t.Name(), chain)
			continue
		}

		t.updateIbcTx(chain, chainConfig, chainMap)
	}
	return 1
}

func (t *AddChainTask) updateIbcTx(chain string, chainConfig *entity.ChainConfig, chainMap map[string]*entity.ChainConfig) {
	logrus.Infof("task %s start updating %s ibc tx", t.Name(), chain)
	if len(chainConfig.IbcInfo) == 0 {
		logrus.Warningf("task %s %s dont't have ibc info", t.Name(), chain)
		return
	}

	for i := range chainConfig.IbcInfo {
		for _, path := range chainConfig.IbcInfo[i].Paths {
			if path.State != constant.ChannelStateOpen {
				logrus.Warningf("task %s %s channel %s is not open", t.Name(), chain, path.ChannelId)
				continue
			}

			clientId := path.ClientId
			var counterpartyClientId string
			counterpartyChain := path.Chain
			counterpartyChannelId := path.Counterparty.ChannelId
			cpChainCfg, ok := chainMap[counterpartyChain]
			if ok {
				counterpartyClientId = cpChainCfg.GetChannelClient(constant.PortTransfer, counterpartyChannelId)
			}

			channelId := path.ChannelId
			var waitGroup sync.WaitGroup
			waitGroup.Add(4)
			go func() {
				defer waitGroup.Done()
				if err := ibcTxRepo.AddNewChainUpdate(counterpartyChain, counterpartyChannelId, counterpartyClientId, chain, clientId); err != nil {
					logrus.Errorf("task %s %s AddNewChainUpdate error, counterpartyChain: %s, counterpartyChannelId: %s", t.Name(), chain, counterpartyChain, counterpartyChannelId)
				}
			}()

			go func() {
				defer waitGroup.Done()
				if err := ibcTxRepo.AddNewChainUpdateFailedTx(counterpartyChain, counterpartyChannelId, counterpartyClientId, chain, channelId, clientId); err != nil {
					logrus.Errorf("task %s %s AddNewChainUpdateFailedTx error, counterpartyChain: %s, counterpartyChannelId: %s", t.Name(), chain, counterpartyChain, counterpartyChannelId)
				}
			}()

			go func() {
				defer waitGroup.Done()
				if err := ibcTxRepo.AddNewChainUpdateHistory(counterpartyChain, counterpartyChannelId, counterpartyClientId, chain, clientId); err != nil {
					logrus.Errorf("task %s %s AddNewChainUpdateHistory error, counterpartyChain: %s, counterpartyChannelId: %s", t.Name(), chain, counterpartyChain, counterpartyChannelId)
				}
			}()

			go func() {
				defer waitGroup.Done()
				if err := ibcTxRepo.AddNewChainUpdateHistoryFailedTx(counterpartyChain, counterpartyChannelId, counterpartyClientId, chain, channelId, clientId); err != nil {
					logrus.Errorf("task %s %s AddNewChainUpdateHistoryFailedTx error, counterpartyChain: %s, counterpartyChannelId: %s", t.Name(), chain, counterpartyChain, counterpartyChannelId)
				}
			}()

			waitGroup.Wait()
		}
	}

	logrus.Infof("task %s update %s ibc tx end", t.Name(), chain)
}

func (t *AddChainTask) updateDenom(denomList entity.IBCDenomList, chainMap map[string]*entity.ChainConfig) {
	logrus.Infof("task %s update denom start", t.Name())
	bulk := denomRepo.Bulk()
	tokenTraceBulk := tokenTraceRepo.Bulk()
	for _, v := range denomList {
		if v.DenomPath == "" || v.RootDenom == "" {
			continue
		}

		denomFullPath := fmt.Sprintf("%s/%s", v.DenomPath, v.RootDenom)
		denomNew := ibctool.TraceDenom(denomFullPath, v.Chain, chainMap)
		if v.BaseDenom != denomNew.BaseDenom || v.BaseDenomChain != denomNew.BaseDenomChain || v.PrevDenom != denomNew.PrevDenom ||
			v.PrevChain != denomNew.PrevChain || v.IsBaseDenom != denomNew.IsBaseDenom {
			logrus.WithField("denom", v).WithField("denom_new", denomNew).Infof("task %s denom trace path is changed", t.Name())
			denomRepo.UpdateDenom(bulk, denomNew)
			tokenTraceRepo.UpdateBaseDenomAndChain(tokenTraceBulk, denomNew.Chain, denomNew.Denom, denomNew.BaseDenom, denomNew.BaseDenomChain)
		}
	}
	bulk.SetOrdered(false)
	tokenTraceBulk.SetOrdered(false)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, err := bulk.Run(context.Background()); err != nil {
			logrus.Errorf("task %s bulk update denom fail, err:%s", t.Name(), err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		if _, err := tokenTraceBulk.Run(context.Background()); err != nil {
			logrus.Errorf("task %s bulk update token trace denomfail, err:%s", t.Name(), err.Error())
		}
	}()
	wg.Wait()

	for _, v := range denomList {
		if v.DenomPath == "" || v.RootDenom == "" {
			continue
		}
		denomFullPath := fmt.Sprintf("%s/%s", v.DenomPath, v.RootDenom)
		denomNew := ibctool.TraceDenom(denomFullPath, v.Chain, chainMap)

		if v.BaseDenom != denomNew.BaseDenom || v.BaseDenomChain != denomNew.BaseDenomChain {
			if err := ibcTxRepo.UpdateBaseDenomInfo(v.BaseDenom, v.BaseDenomChain, denomNew.BaseDenom, denomNew.BaseDenomChain); err != nil {
				logrus.Errorf("task %s UpdateBaseDenomInfo error, %s-%s => %s-%s", t.Name(), v.BaseDenomChain, v.BaseDenom, denomNew.BaseDenomChain, denomNew.BaseDenom)
			}
			if err := ibcTxRepo.UpdateBaseDenomInfoHistory(v.BaseDenom, v.BaseDenomChain, denomNew.BaseDenom, denomNew.BaseDenomChain); err != nil {
				logrus.Errorf("task %s UpdateBaseDenomInfoHistory error, %s-%s => %s-%s", t.Name(), v.BaseDenomChain, v.BaseDenom, denomNew.BaseDenomChain, denomNew.BaseDenom)
			}
		}
	}

	logrus.Infof("task %s update denom end", t.Name())
}
