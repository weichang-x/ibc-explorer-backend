package task

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/irisnet/ibc-explorer-backend/internal/app/model/vo"
	"github.com/qiniu/qmgo"

	"github.com/irisnet/ibc-explorer-backend/internal/app/constant"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/dto"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/irisnet/ibc-explorer-backend/internal/app/utils"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

type ChannelTask struct {
	allChannelIds    []string
	channelStatusMap map[string]entity.ChannelStatus
	baseDenomMap     entity.IBCBaseDenomMap
	chainTxsMap      map[string]int64
	chainTxsValueMap map[string]decimal.Decimal
}

func (t *ChannelTask) Name() string {
	return "ibc_channel_task"
}

func (t *ChannelTask) Cron() int {
	if taskConf.CronTimeChannelTask > 0 {
		return taskConf.CronTimeChannelTask
	}
	return ThreeMinute
}

func (t *ChannelTask) Run() int {
	t.clear()
	if err := t.analyzeChainConfig(); err != nil {
		return -1
	}

	existedChannelList, newChannelList, removedChannelIds, err := t.getAllChannel()
	if err != nil {
		return -1
	}

	_ = t.setLatestSettlementTime(existedChannelList, newChannelList)

	t.setStatusAndOperatingPeriod(existedChannelList, newChannelList)

	_ = t.todayStatistics()

	_ = t.yesterdayStatistics()

	baseDenomList, err := authDenomRepo.FindAll()
	if err != nil {
		logrus.Errorf("task %s run error, %v", t.Name(), err)
		return -1
	}
	t.baseDenomMap = baseDenomList.ConvertToMap()

	if err = t.setTransferTxs(existedChannelList, newChannelList); err != nil {
		logrus.Errorf("task %s setTransferTxs error, %v", t.Name(), err)
		return -1
	}

	chainConfList, err := chainConfigRepo.FindAllChainInfos()
	if err != nil {
		logrus.Errorf("task %s chainConfList error, %v", t.Name(), err)
		return -1
	}

	chainCfgMap := make(map[string]*entity.ChainConfig, len(chainConfList))
	for _, v := range chainConfList {
		if v.Status == entity.ChannelStatusClosed {
			continue
		}
		chainCfgMap[v.ChainName] = v
	}

	if err = t.setPendingTxs(chainCfgMap, existedChannelList, newChannelList); err != nil {
		logrus.Errorf("task %s setPendingTxs error, %v", t.Name(), err)
		return -1
	}

	if len(newChannelList) > 0 {
		if err = channelRepo.InsertBatch(newChannelList); err != nil {
			logrus.Errorf("task %s InsertBatch error, %v", t.Name(), err)
		}
	}

	if len(removedChannelIds) > 0 {
		if err = channelRepo.DeleteByChannelIds(removedChannelIds); err != nil {
			logrus.Errorf("task %s DeleteByChannelIds error, %v", t.Name(), err)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		if length := len(existedChannelList); length > 1000 {
			batchSize := constant.DefaultLimit
			for i := 0; i < length; i += batchSize {
				end := i + batchSize
				if end > length {
					end = length
				}
				batch := existedChannelList[i:end]
				err := bulkAppendAndRun(entity.IBCChannel{}.CollectionName(), func(bulk *qmgo.Bulk) {
					for _, v := range batch {
						channelRepo.UpdateChannel(bulk, v)
					}
				})
				if err != nil && err != mongo.ErrNoDocuments {
					logrus.Errorf("task %s bulk UpdateChannel error, %s", t.Name(), err.Error())
				}
				logrus.Infof("task %s batch bulk UpdateChannel index:%d end:%d total:%d", t.Name(), i, end, length)
			}
		} else {
			channelBulk := channelRepo.Bulk()
			for _, v := range existedChannelList {
				channelRepo.UpdateChannel(channelBulk, v)
			}
			channelBulk.SetOrdered(false)
			if _, err := channelBulk.Run(context.Background()); err != nil && err != mongo.ErrNoDocuments {
				logrus.Errorf("task %s batch bulk UpdateChannel error, %s", t.Name(), err.Error())
			}
		}
	}()

	go func() {
		defer wg.Done()
		chainBulk := chainRepo.Bulk()
		for chain, txs := range t.chainTxsMap {
			txsValue := t.chainTxsValueMap[chain].Round(constant.DefaultValuePrecision).String()
			chainRepo.UpdateTransferTxs(chainBulk, chain, txs, txsValue)
		}
		chainBulk.SetOrdered(false)
		if _, err := chainBulk.Run(context.Background()); err != nil && err != mongo.ErrNoDocuments {
			logrus.Errorf("task %s bulk update chain transfer_txs error, %s", t.Name(), err.Error())
		}
	}()

	wg.Wait()
	return 1
}

func (t *ChannelTask) clear() {
	t.chainTxsMap = make(map[string]int64)
	t.chainTxsValueMap = make(map[string]decimal.Decimal)
}

func (t *ChannelTask) analyzeChainConfig() error {
	confList, err := chainConfigRepo.FindAll()
	if err != nil {
		logrus.Errorf("task %s analyzeChainConfig error, %v", t.Name(), err)
		return err
	}

	var channelIds []string
	channelStatusMap := make(map[string]entity.ChannelStatus)

	var chainA, channelA, chainB, channelB string
	for i := range confList {
		chainA = confList[i].ChainName
		for _, info := range confList[i].IbcInfo {
			chainB = info.Chain
			for _, p := range info.Paths {
				channelA = p.ChannelId
				channelB = p.Counterparty.ChannelId
				channelId := generateChannelId(chainA, channelA, chainB, channelB)

				if utils.InArray(channelIds, channelId) {
					continue
				}

				channelIds = append(channelIds, channelId)
				if p.State == constant.ChannelStateOpen && p.Counterparty.State == constant.ChannelStateOpen {
					channelStatusMap[channelId] = entity.ChannelStatusOpened
				} else {
					channelStatusMap[channelId] = entity.ChannelStatusClosed
				}
			}
		}
	}

	t.allChannelIds = channelIds
	t.channelStatusMap = channelStatusMap
	return nil
}

func generateChannelId(chainA, channelA, chainB, channelB string) string {
	if strings.HasPrefix(strings.ToLower(chainA), constant.Cosmos) {
		return fmt.Sprintf("%s|%s|%s|%s", chainA, channelA, chainB, channelB)
	}

	if strings.HasPrefix(strings.ToLower(chainB), constant.Cosmos) {
		return fmt.Sprintf("%s|%s|%s|%s", chainB, channelB, chainA, channelA)
	}

	if strings.HasPrefix(strings.ToLower(chainA), constant.Iris) {
		return fmt.Sprintf("%s|%s|%s|%s", chainA, channelA, chainB, channelB)
	}

	if strings.HasPrefix(strings.ToLower(chainB), constant.Iris) {
		return fmt.Sprintf("%s|%s|%s|%s", chainB, channelB, chainA, channelA)
	}

	compare := strings.Compare(strings.ToLower(chainA), strings.ToLower(chainB))
	if compare < 0 {
		return fmt.Sprintf("%s|%s|%s|%s", chainA, channelA, chainB, channelB)
	} else {
		return fmt.Sprintf("%s|%s|%s|%s", chainB, channelB, chainA, channelA)
	}
}

func (t *ChannelTask) parseChannelId(channelId string) (chainA, channelA, chainB, channelB string, err error) {
	split := strings.Split(channelId, "|")
	if len(split) != 4 {
		logrus.Errorf("task %s parseChannelId error, %v", t.Name(), err)
		return "", "", "", "", fmt.Errorf("channel id format error")
	}
	return split[0], split[1], split[2], split[3], nil
}

func (t *ChannelTask) getAllChannel() (entity.IBCChannelList, entity.IBCChannelList, []string, error) {
	existedChannelList, err := channelRepo.FindAll()
	if err != nil {
		logrus.Errorf("task %s getAllChannel error, %v", t.Name(), err)
		return nil, nil, nil, err
	}

	var newChannelList, stillExistChannelList entity.IBCChannelList
	for _, v := range t.allChannelIds {
		isExist := false
		for _, e := range existedChannelList {
			if v == e.ChannelId {
				stillExistChannelList = append(stillExistChannelList, e)
				isExist = true
				break
			}
		}

		if isExist {
			continue
		}

		chainA, channelA, chainB, channelB, err := t.parseChannelId(v)
		if err != nil {
			return nil, nil, nil, err
		}

		newChannelList = append(newChannelList, &entity.IBCChannel{
			ChannelId:        v,
			ChainA:           chainA,
			ChainB:           chainB,
			ChannelA:         channelA,
			ChannelB:         channelB,
			Status:           entity.ChannelStatusOpened,
			OperatingPeriod:  0,
			LatestOpenTime:   0,
			PendingTxs:       0,
			TransferTxs:      0,
			TransferTxsValue: "",
			CreateAt:         time.Now().Unix(),
			UpdateAt:         time.Now().Unix(),
		})
	}

	var removedChannelIds []string
	for _, v := range existedChannelList {
		if !utils.InArray(t.allChannelIds, v.ChannelId) {
			removedChannelIds = append(removedChannelIds, v.ChannelId)
		}
	}

	return stillExistChannelList, newChannelList, removedChannelIds, nil
}

func (t *ChannelTask) setLatestSettlementTime(existedChannelList entity.IBCChannelList, newChannelList entity.IBCChannelList) error {
	for _, v := range newChannelList {
		if chanConf, err := channelConfigRepo.Find(v.ChainA, v.ChannelA, v.ChainB, v.ChannelB); err == nil {
			v.LatestOpenTime = chanConf.ChannelOpenAt
			continue
		}

		if ct, err := t.queryChannelOpenConfirmTime(v.ChainA, v.ChannelA, v.ChainB, v.ChannelB); err == nil {
			v.LatestOpenTime = ct
		}
	}

	for _, v := range existedChannelList {
		if v.LatestOpenTime == 0 && v.Status == entity.ChannelStatusOpened {
			if chanConf, err := channelConfigRepo.Find(v.ChainA, v.ChannelA, v.ChainB, v.ChannelB); err == nil {
				v.LatestOpenTime = chanConf.ChannelOpenAt
			} else if time.Now().Unix()-v.CreateAt < int64(48*time.Hour) {
				if ct, err := t.queryChannelOpenConfirmTime(v.ChainA, v.ChannelA, v.ChainB, v.ChannelB); err == nil {
					v.LatestOpenTime = ct
				}
			}
		}

		if v.Status == entity.ChannelStatusClosed && t.channelStatusMap[v.ChannelId] == entity.ChannelStatusOpened {
			if openTime, err := t.queryChannelOpenConfirmTime(v.ChainA, v.ChannelA, v.ChainB, v.ChannelB); err == nil {
				v.LatestOpenTime = openTime
			}
		}
	}
	return nil
}

func (t *ChannelTask) queryChannelOpenConfirmTime(chainA, channelA, chainB, channelB string) (int64, error) {
	if confirmTime, err := txRepo.GetChannelOpenConfirmTime(chainA, channelA); err != nil {
		return txRepo.GetChannelOpenConfirmTime(chainB, channelB)
	} else {
		return confirmTime, err
	}
}

func (t *ChannelTask) setStatusAndOperatingPeriod(existedChannelList entity.IBCChannelList, newChannelList entity.IBCChannelList) {
	set := func(list entity.IBCChannelList) {
		for _, v := range list {
			currentStatus, ok := t.channelStatusMap[v.ChannelId]
			if !ok {
				currentStatus = entity.ChannelStatusOpened
			}

			if v.LatestOpenTime == 0 {
				v.Status = currentStatus
				continue
			}

			if v.Status == entity.ChannelStatusClosed && currentStatus == entity.ChannelStatusClosed {
				continue
			}

			now := time.Now().Unix()
			v.OperatingPeriod = now - v.LatestOpenTime
			v.Status = currentStatus
		}
	}

	set(existedChannelList)
	set(newChannelList)
}

func (t *ChannelTask) setTransferTxs(existedChannelList entity.IBCChannelList, newChannelList entity.IBCChannelList) error {
	statistics, err := channelStatisticsRepo.Aggr()
	if err != nil {
		logrus.Errorf("task %s channelStatisticsRepo.Aggr error, %v", t.Name(), err)
		return err
	}

	for _, v := range existedChannelList {
		count, value := t.calculateChannelStatistics(v.ChannelId, statistics)
		v.TransferTxs = count
		v.TransferTxsValue = value.Round(constant.DefaultValuePrecision).String()
	}

	for _, v := range newChannelList {
		count, value := t.calculateChannelStatistics(v.ChannelId, statistics)
		v.TransferTxs = count
		v.TransferTxsValue = value.Round(constant.DefaultValuePrecision).String()
	}

	return nil
}

func (t *ChannelTask) setPendingTxs(chainCfgMap map[string]*entity.ChainConfig, existedChannelList entity.IBCChannelList, newChannelList entity.IBCChannelList) error {
	chainPendingTxs := func(channel *entity.IBCChannel) {
		wg := sync.WaitGroup{}
		var chainAPendingTxs, chainBPendingTxs int
		wg.Add(2)
		go func() {
			defer wg.Done()
			chainACfg := chainCfgMap[channel.ChainA]
			if chainACfg != nil && channel.ChannelA != "" {
				//todo not only support 'transfer' port
				//pendingTxCnt, err := t.getPengingTxsFromLcd(channel.ChannelA, "transfer", chainACfg.GrpcRestGateway, chainACfg.LcdApiPath.PacketCommitsPath)
				pendingTxCnt, err := t.getPengingTxsFromLcd(channel.ChannelA, "transfer", chainACfg.GrpcRestGateway, strings.ReplaceAll(chainACfg.LcdApiPath.ClientStatePath, "client_state", "packet_commitments"))
				if err != nil {
					logrus.Error(err.Error())
					return
				}
				if pendingTxCnt != nil {
					chainAPendingTxs = *pendingTxCnt
				}
			}
		}()
		go func() {
			defer wg.Done()
			chainBCfg := chainCfgMap[channel.ChainB]
			if chainBCfg != nil && channel.ChannelB != "" {
				//todo not only support 'transfer' port
				//pendingTxCnt, err := t.getPengingTxsFromLcd(channel.ChannelB, "transfer", chainBCfg.GrpcRestGateway, chainBCfg.LcdApiPath.PacketCommitsPath)
				pendingTxCnt, err := t.getPengingTxsFromLcd(channel.ChannelB, "transfer", chainBCfg.GrpcRestGateway, strings.ReplaceAll(chainBCfg.LcdApiPath.ClientStatePath, "client_state", "packet_commitments"))
				if err != nil {
					logrus.Error(err.Error())
					return
				}
				if pendingTxCnt != nil {
					chainBPendingTxs = *pendingTxCnt
				}
			}
		}()
		wg.Wait()
		channel.PendingTxs = chainAPendingTxs + chainBPendingTxs
		return
	}

	doHandle := func(workNum int, channels []*entity.IBCChannel, dowork func(channel *entity.IBCChannel)) {
		var wg sync.WaitGroup
		wg.Add(workNum)
		for i := 0; i < workNum; i++ {
			num := i
			go func(num int) {
				defer wg.Done()

				for id := range channels {
					if id%workNum != num {
						continue
					}
					dowork(channels[id])
				}
			}(num)
		}
		wg.Wait()
	}
	doHandle(5, existedChannelList, chainPendingTxs)
	doHandle(5, newChannelList, chainPendingTxs)
	return nil
}

func (t *ChannelTask) getPengingTxsFromLcd(channel, port, lcd, apiPath string) (*int, error) {
	apiPath = strings.ReplaceAll(apiPath, replaceHolderChannel, channel)
	apiPath = strings.ReplaceAll(apiPath, replaceHolderPort, port)
	url := fmt.Sprintf("%s%s", lcd, apiPath)

	bz, err := utils.HttpGet(url)
	if err != nil {
		return nil, err
	}

	var resp vo.IbcPacketCommitsResp
	err = json.Unmarshal(bz, &resp)
	if err != nil {
		return nil, err
	}
	return &resp.Pagination.Total, nil
}

func (t *ChannelTask) calculateChannelStatistics(channelId string, statistics []*dto.ChannelStatisticsAggrDTO) (int64, decimal.Decimal) {
	var txsCount int64 = 0
	var txsValue = decimal.Zero

	for _, v := range statistics {
		if channelId == v.ChannelId {
			valueDecimal := t.calculateValue(v.TxsAmount, v.BaseDenom, v.BaseDenomChain)
			txsCount += v.TxsCount
			txsValue = txsValue.Add(valueDecimal)

			chainA, _, chainB, _, _ := t.parseChannelId(channelId)
			t.chainTxsMap[chainA] += v.TxsCount
			t.chainTxsMap[chainB] += v.TxsCount
			d, ok := t.chainTxsValueMap[chainA]
			if ok {
				t.chainTxsValueMap[chainA] = d.Add(valueDecimal)
			} else {
				t.chainTxsValueMap[chainA] = valueDecimal
			}

			d, ok = t.chainTxsValueMap[chainB]
			if ok {
				t.chainTxsValueMap[chainB] = d.Add(valueDecimal)
			} else {
				t.chainTxsValueMap[chainB] = valueDecimal
			}
		}
	}

	return txsCount, txsValue
}

func (t *ChannelTask) calculateValue(amount float64, baseDenom, baseDenomChain string) decimal.Decimal {
	denom, ok := t.baseDenomMap[fmt.Sprintf("%s%s", baseDenomChain, baseDenom)]
	if !ok || denom.CoinId == "" {
		return decimal.Zero
	}

	price, err := tokenPriceRepo.Get(denom.CoinId)
	if err != nil {
		logrus.Errorf("task %s calculateValue error, %v", t.Name(), err)
		return decimal.Zero
	}

	value := decimal.NewFromFloat(amount).Div(decimal.NewFromFloat(math.Pow10(denom.Scale))).
		Mul(decimal.NewFromFloat(price))

	return value
}

func (t *ChannelTask) todayStatistics() error {
	logrus.Infof("task %s exec today statistics", t.Name())
	startTime, endTime := todayUnix()
	segments := []*segment{
		{
			StartTime: startTime,
			EndTime:   endTime,
		},
	}
	if err := ChannelIncrementStatistics(segments); err != nil {
		logrus.Errorf("task %s todayStatistics error, %v", t.Name(), err)
		return err
	}

	return nil
}

func (t *ChannelTask) yesterdayStatistics() error {
	ok, seg := whetherCheckYesterdayStatistics(t.Name(), t.Cron())
	if !ok {
		return nil
	}

	logrus.Infof("task %s check yeaterday statistics", t.Name())
	if err := ChannelIncrementStatistics([]*segment{seg}); err != nil {
		logrus.Errorf("task %s yesterdayStatistics error, %v", t.Name(), err)
		return err
	}

	return nil
}
