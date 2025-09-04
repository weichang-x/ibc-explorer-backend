package task

import (
	"context"
	"fmt"

	"github.com/irisnet/ibc-explorer-backend/internal/app/constant"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/sirupsen/logrus"
)

type IbcDenomUpdateTask struct {
}

var _ Task = new(IbcDenomUpdateTask)

func (t *IbcDenomUpdateTask) Name() string {
	return "ibc_denom_update_task"
}

func (t *IbcDenomUpdateTask) Cron() int {
	if taskConf.CronTimeDenomUpdateTask > 0 {
		return taskConf.CronTimeDenomUpdateTask
	}
	return ThreeMinute
}

func (t *IbcDenomUpdateTask) Run() int {
	denomSymbolMap, err := t.getBaseDenomSysbolMap()
	if err != nil {
		return -1
	}

	if len(denomSymbolMap) == 0 {
		return 1
	}

	if err = t.handleIbcDenoms(denomSymbolMap); err != nil {
		return -1
	}

	return 1
}

func (t *IbcDenomUpdateTask) getBaseDenomSysbolMap() (map[string]string, error) {
	baseDenomList, err := authDenomRepo.FindAll()
	if err != nil {
		logrus.Errorf("task %s authDenomRepo.FindAll error, %v", t.Name(), err)
		return nil, err
	}

	denomSymbolMap := make(map[string]string, len(baseDenomList))
	for _, v := range baseDenomList {
		denomSymbolMap[fmt.Sprintf("%s%s", v.Chain, v.Denom)] = v.Symbol
	}
	return denomSymbolMap, nil
}

func (t *IbcDenomUpdateTask) handleIbcDenoms(denomSymbolMap map[string]string) error {
	createAt := int64(0)
	for {
		denomList, err := denomRepo.FindNoSymbolDenoms(createAt, constant.DefaultLimit)
		if err != nil {
			logrus.Errorf("task %s denomRepo.FindNoSymbolDenoms error, %v", t.Name(), err)
			return err
		}
		bulk := denomRepo.Bulk()
		var change bool
		for _, v := range denomList {
			symbol, ok := denomSymbolMap[fmt.Sprintf("%s%s", v.BaseDenomChain, v.BaseDenom)]
			if ok {
				denomRepo.UpdateSymbol(bulk, v.Chain, v.Denom, symbol)
				change = true
			}
		}
		if change {
			bulk.SetOrdered(false)
			if _, err := bulk.Run(context.Background()); err != nil && err != mongo.ErrNoDocuments {
				logrus.Errorf("task %s bulk UpdateSymbol error, %s", t.Name(), err.Error())
			}
		}
		createAt = denomList[len(denomList)-1].CreateAt
		if len(denomList) <= constant.DefaultLimit {
			break
		}
	}
	return nil
}
