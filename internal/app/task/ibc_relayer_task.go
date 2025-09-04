package task

/***
  ibc_relayer_task 定时任务
  功能范围：
      1.根据已注册的relayer的地址、链信息，更新channel_pair_info字段。
      2.更新relayer的update_time。
      3.更新channel页面relayer的数量、channel的更新时间、chain页面relayer数量。
      4.增量更新(包括已注册,未注册)relayer相关信息(交易总数、成功交易总数、relayer费用总价值、交易总价值)。
      5.relayer address 归档到relayer
*/
import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/irisnet/ibc-explorer-backend/internal/app/constant"
	"github.com/qiniu/qmgo"

	"github.com/irisnet/ibc-explorer-backend/internal/app/model/dto"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/vo"
	"github.com/irisnet/ibc-explorer-backend/internal/app/pkg/lcd"
	"github.com/irisnet/ibc-explorer-backend/internal/app/repository"
	"github.com/irisnet/ibc-explorer-backend/internal/app/repository/cache"
	"github.com/irisnet/ibc-explorer-backend/internal/app/utils"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

type IbcRelayerCronTask struct {
	chainConfigMap map[string]*entity.ChainConfig
	//key: BaseDenom+Chain
	denomPriceMap        map[string]dto.CoinItem
	channelUpdateTimeMap *sync.Map
}

func (t *IbcRelayerCronTask) Name() string {
	return "ibc_relayer_task"
}
func (t *IbcRelayerCronTask) Cron() int {
	if taskConf.CronTimeRelayerTask > 0 {
		return taskConf.CronTimeRelayerTask
	}
	return ThreeMinute
}

func (t *IbcRelayerCronTask) Run() int {
	if err := t.init(); err != nil {
		return -1
	}

	t.denomPriceMap = cache.TokenPriceMap()
	_ = t.todayStatistics()
	_ = t.yesterdayStatistics()
	t.addressGather()

	t.CheckAndChangeRelayer()
	t.updateIbcChainsRelayer()

	return 1
}

func (t *IbcRelayerCronTask) init() error {
	if chainConfigMap, err := getAllChainMap(); err != nil {
		logrus.Errorf("task %s getAllChainMap err, %v", t.Name(), err)
		return err
	} else {
		t.chainConfigMap = chainConfigMap
	}

	t.channelUpdateTimeMap = new(sync.Map)
	return nil
}

func (t *IbcRelayerCronTask) updateRelayerUpdateTime(bulk *qmgo.Bulk, relayer *entity.IBCRelayerNew) {
	//get latest update_client time
	updateTime := t.getUpdateTime(relayer)
	if relayer.UpdateTime >= updateTime {
		return
	}
	if err := relayerRepo.UpdateRelayerTime(bulk, relayer.RelayerId, updateTime); err != nil {
		logrus.Error("update relayer about update_time fail, ", err.Error())
	}
}
func (t *IbcRelayerCronTask) CheckAndChangeRelayer() {
	handleRelayers := func(workNum int, relayers []*entity.IBCRelayerNew, dowork func(one *entity.IBCRelayerNew)) {
		var wg sync.WaitGroup
		wg.Add(workNum)
		for i := 0; i < workNum; i++ {
			num := i
			go func(num int) {
				defer wg.Done()
				for id := range relayers {
					if id%workNum != num {
						continue
					}
					dowork(relayers[id])
				}
			}(num)
		}
		wg.Wait()
	}

	skip := int64(0)
	limit := int64(1000)
	for {
		relayers, err := relayerRepo.FindAll(skip, limit, repository.RelayerAllType)
		if err != nil {
			logrus.Error("find relayer by page fail, ", err.Error())
			return
		}
		handleRelayers(5, relayers, t.updateOneRelayer)

		if len(relayers) < int(limit) {
			break
		}
		skip += limit
	}
}

func (t *IbcRelayerCronTask) updateOneRelayer(one *entity.IBCRelayerNew) {
	rlyBulk := relayerRepo.Bulk()
	t.handleRelayerChannelPair(rlyBulk, one)
	t.handleRelayerStatistic(rlyBulk, t.denomPriceMap, one)
	t.updateRelayerUpdateTime(rlyBulk, one)
	rlyBulk.SetOrdered(false)
	if _, err := rlyBulk.Run(context.Background()); err != nil && err != mongo.ErrNoDocuments {
		logrus.Errorf("task %s bulk update relayer channel_pair,updateTime, statistic error, %s", t.Name(), err.Error())
	}

	var change bool
	bulkChannel := channelRepo.Bulk()
	for _, channelPair := range one.ChannelPairInfo {
		channelId := generateChannelId(channelPair.ChainA, channelPair.ChannelA, channelPair.ChainB, channelPair.ChannelB)
		ok := t.updateIbcChannelRelayerInfo(bulkChannel, channelId)
		if !change {
			change = ok
		}
	}
	if change {
		bulkChannel.SetOrdered(false)
		if _, err := bulkChannel.Run(context.Background()); err != nil && err != mongo.ErrNoDocuments {
			logrus.Errorf("task %s bulk updateIbcChannelRelayerInfo error, %s", t.Name(), err.Error())
		}
	}
}

func (t *IbcRelayerCronTask) updateIbcChannelRelayerInfo(bulk *qmgo.Bulk, channelId string) bool {
	var change bool
	if channelId != "" {
		value, ok := t.channelUpdateTimeMap.Load(channelId)
		if ok && value.(int64) > 0 {
			change = true
			if err := channelRepo.UpdateOneUpdateTime(bulk, channelId, value.(int64)); err != nil && err != mongo.ErrNoDocuments {
				logrus.Error("update ibc_channel updateTime fail, ", err.Error())
			}
		}

	}
	return change
}

func AggrRelayerTxsAndAmt(relayerNew *entity.IBCRelayerNew) map[string]dto.TxsAmtItem {
	combs := entity.ChannelPairInfoList(relayerNew.ChannelPairInfo).GetChainAddrCombs()
	res, err := relayerDenomStatisticsRepo.AggrRelayerBaseDenomAmtAndTxs(combs)
	if err != nil {
		logrus.Error("aggregate relayer txs have fail, ", err.Error(),
			" relayer_id: ", relayerNew.RelayerId,
			" relayer_name: ", relayerNew.RelayerName)
		return nil
	}
	relayerTxsAmtMap := make(map[string]dto.TxsAmtItem, 20)
	for _, item := range res {
		key := fmt.Sprintf("%s%s", item.BaseDenom, item.BaseDenomChain)
		value, exist := relayerTxsAmtMap[key]
		if exist {
			value.Txs += item.TotalTxs
			value.Amt = value.Amt.Add(decimal.NewFromFloat(item.Amount))
			if item.TxStatus == int(entity.TxStatusSuccess) {
				value.TxsSuccess += item.TotalTxs
			}
			relayerTxsAmtMap[key] = value
		} else {
			data := dto.TxsAmtItem{
				Chain: item.BaseDenomChain,
				Denom: item.BaseDenom,
				Txs:   item.TotalTxs,
				Amt:   decimal.NewFromFloat(item.Amount),
			}
			if item.TxStatus == int(entity.TxStatusSuccess) {
				data.TxsSuccess = item.TotalTxs
			}
			relayerTxsAmtMap[key] = data
		}
	}
	return relayerTxsAmtMap
}

func AggrRelayerFeeAmt(relayerNew *entity.IBCRelayerNew) map[string]dto.TxsAmtItem {
	addrCombs := entity.ChannelPairInfoList(relayerNew.ChannelPairInfo).GetChainAddrCombs()
	res, err := relayerFeeStatisticsRepo.AggrRelayerFeeDenomAmt(addrCombs)
	if err != nil {
		logrus.Error("aggregate relayer txs have fail, ", err.Error(),
			" relayer_id: ", relayerNew.RelayerId,
			" relayer_name: ", relayerNew.RelayerName)
		return nil
	}
	relayerTxsAmtMap := make(map[string]dto.TxsAmtItem, 20)
	for _, item := range res {
		key := fmt.Sprintf("%s%s", item.FeeDenom, item.Chain)
		value, exist := relayerTxsAmtMap[key]
		if exist {
			value.Txs += item.TotalTxs
			value.Amt = value.Amt.Add(decimal.NewFromFloat(item.Amount))
			relayerTxsAmtMap[key] = value
		} else {
			data := dto.TxsAmtItem{
				Chain: item.Chain,
				Denom: item.FeeDenom,
				Txs:   item.TotalTxs,
				Amt:   decimal.NewFromFloat(item.Amount),
			}
			relayerTxsAmtMap[key] = data
		}
	}
	return relayerTxsAmtMap
}

// dependence: AggrRelayerFeeAmt or AggrRelayerTxsAndAmt
func caculateRelayerTotalValue(denomPriceMap map[string]dto.CoinItem, relayerTxsDataMap map[string]dto.TxsAmtItem) decimal.Decimal {
	return dto.CaculateRelayerTotalValue(denomPriceMap, relayerTxsDataMap)
}

func (t *IbcRelayerCronTask) updateIbcChainsRelayer() {
	res, err := chainCache.FindAll()
	if err != nil {
		logrus.Error("find ibc_chains data fail, ", err.Error())
		return
	}

	bulk := chainRepo.Bulk()
	for _, val := range res {
		relayerCnt, err := relayerRepo.CountChainRelayers(val.Chain)
		if err != nil {
			logrus.Error("count relayers of chain fail, ", err.Error())
			continue
		}
		chainRepo.UpdateRelayers(bulk, val.Chain, relayerCnt)
	}
	bulk.SetOrdered(false)
	if _, err := bulk.Run(context.Background()); err != nil && err != mongo.ErrNoDocuments {
		logrus.Errorf("task %s bulk ibc_chain relayers error, %s", t.Name(), err.Error())
	}
	return
}

// 1: updateTime
func (t *IbcRelayerCronTask) getUpdateTime(relayer *entity.IBCRelayerNew) int64 {
	var startTime int64

	//use unbonding_time
	startTime = time.Now().Add(-24 * 21 * time.Hour).Unix()
	if relayer.UpdateTime > 0 && relayer.UpdateTime <= startTime {
		startTime = relayer.UpdateTime

	}

	getChannelPairUpdateTime := func(channelPair entity.ChannelPairInfo) (int64, string) {
		var updateTimeA, updateTimeB int64
		var clientIdA, clientIdB string
		var err error
		group := sync.WaitGroup{}
		group.Add(2)
		go func() {
			defer group.Done()
			clientIdA, err = t.getChannelClient(channelPair.ChainA, channelPair.ChannelA)
			if err != nil {
				logrus.Warnf("get channel client fail, %s", err.Error())
				return
			}
			updateTimeA, err = txRepo.GetUpdateTimeByUpdateClient(channelPair.ChainA, channelPair.ChainAAddress, clientIdA, startTime)
			if err != nil {
				logrus.Warnf("get channel pairInfo updateTime fail, %s", err.Error())
			}
		}()

		go func() {
			defer group.Done()
			clientIdB, err = t.getChannelClient(channelPair.ChainB, channelPair.ChannelB)
			if err != nil {
				logrus.Warnf("get channel client fail, %s", err.Error())
				return
			}
			updateTimeB, err = txRepo.GetUpdateTimeByUpdateClient(channelPair.ChainB, channelPair.ChainBAddress, clientIdB, startTime)
			if err != nil {
				logrus.Warnf("get channel pairInfo updateTime fail, %s", err.Error())
			}
		}()
		group.Wait()
		channelId := generateChannelId(channelPair.ChainA, channelPair.ChannelA, channelPair.ChainB, channelPair.ChannelB)

		if updateTimeA >= updateTimeB {
			return updateTimeA, channelId
		}
		return updateTimeB, channelId
	}

	dochannelPairInfos := func(workNum int, pairInfos []entity.ChannelPairInfo, dowork func(one entity.ChannelPairInfo, pos int)) {
		var wg sync.WaitGroup
		wg.Add(workNum)
		for i := 0; i < workNum; i++ {
			num := i
			go func(num int) {
				defer wg.Done()
				for id := range pairInfos {
					if id%workNum != num {
						continue
					}
					dowork(pairInfos[id], id)
				}
			}(num)
		}
		wg.Wait()
	}

	updateTimes := make([]int64, len(relayer.ChannelPairInfo))
	dochannelPairInfos(3, relayer.ChannelPairInfo, func(one entity.ChannelPairInfo, pos int) {
		updateTime, channelId := getChannelPairUpdateTime(one)
		t.channelUpdateTimeMap.Store(channelId, updateTime)
		updateTimes[pos] = updateTime
	})
	var relayerUpdateTime int64
	for i := range updateTimes {
		if updateTimes[i] > relayerUpdateTime {
			relayerUpdateTime = updateTimes[i]
		}
	}

	return relayerUpdateTime
}

func (t *IbcRelayerCronTask) getChannelClient(chain, channelId string) (string, error) {
	chainConf, ok := t.chainConfigMap[chain]
	if !ok {
		return "", fmt.Errorf("%s config not found", chain)
	}

	port := chainConf.GetPortId(channelId)
	if port == "" {
		port = constant.PortTransfer
	}
	state, err := lcd.QueryClientState(chainConf.GrpcRestGateway, chainConf.LcdApiPath.ClientStatePath, port, channelId)
	if err != nil {
		return "", err
	}

	return state.IdentifiedClientState.ClientId, nil
}

func (t *IbcRelayerCronTask) todayStatistics() error {
	logrus.Infof("task %s exec today statistics", t.Name())
	startTime, endTime := todayUnix()
	segments := []*segment{
		{
			StartTime: startTime,
			EndTime:   endTime,
		},
	}
	if err := relayerStatisticsTask.RunIncrement(segments[0]); err != nil {
		logrus.Errorf("task %s todayStatistics error, %v", t.Name(), err)
		return err
	}

	return nil
}

func (t *IbcRelayerCronTask) yesterdayStatistics() error {
	ok, seg := whetherCheckYesterdayStatistics(t.Name(), t.Cron())
	if !ok {
		return nil
	}

	logrus.Infof("task %s check yeaterday statistics", t.Name())
	if err := relayerStatisticsTask.RunIncrement(seg); err != nil {
		logrus.Errorf("task %s todayStatistics error, %v", t.Name(), err)
		return err
	}

	return nil
}

func (t *IbcRelayerCronTask) addressGather() {
	_ = relayerAddressGatherTask.Run()
}

func (t *IbcRelayerCronTask) handleRelayerChannelPair(bulk *qmgo.Bulk, relayer *entity.IBCRelayerNew) {
	channelPairs, change1, err := singleSideAddressMatchPair(relayer.ChannelPairInfo)
	if err != nil {
		logrus.Errorf("task %s singleSideAddressMatchPair fail, %v", t.Name(), err.Error())
		return
	}

	channelPairs, change2, err := matchRelayerChannelPairInfo(channelPairs)
	if err != nil {
		logrus.Errorf("task %s matchRelayerChannelPairInfo fail, %v", t.Name(), err.Error())
		return
	}

	if change1 || change2 {
		relayer.ChannelPairInfo = channelPairs
		if err := relayerRepo.UpdateChannelPairInfo(bulk, relayer.RelayerId, relayer.ChannelPairInfo); err != nil {
			logrus.Errorf("task %s update register relayer(%s) statistic fail, %v", t.Name(), relayer.RelayerId, err.Error())
		}
	}
	return
}

func (t *IbcRelayerCronTask) handleRelayerStatistic(bulk *qmgo.Bulk, denomPriceMap map[string]dto.CoinItem, relayer *entity.IBCRelayerNew) {
	item := getRelayerStatisticData(denomPriceMap, relayer)
	if err := relayerRepo.UpdateTxsInfo(bulk, item.RelayerId, item.RelayedTotalTxs, item.RelayedSuccessTxs,
		item.RelayedTotalTxsValue, item.TotalFeeValue); err != nil {
		logrus.Errorf("task %s update register relayer statistic fail, %v", t.Name(), err.Error())
	}
}

func singleSideAddressMatchPair(pairInfoList []entity.ChannelPairInfo) ([]entity.ChannelPairInfo, bool, error) {
	genKey := func(chain, channel, address, pairId string) string {
		return fmt.Sprintf("%s:%s:%s:%s", chain, channel, address, pairId)
	}

	splitKey := func(key string) (string, string, string, string) {
		split := strings.Split(key, ":")
		return split[0], split[1], split[2], split[3]
	}

	pairIdMap := make(map[string]struct{}, len(pairInfoList))
	allAddrChainCombs := make([]string, 0, len(pairInfoList))
	var singleSideAddrChainCombs []string
	for _, v := range pairInfoList {
		pairIdMap[v.PairId] = struct{}{}
		allAddrChainCombs = append(allAddrChainCombs, genKey(v.ChainA, v.ChannelA, v.ChainAAddress, v.PairId))
		if v.ChainB == "" {
			singleSideAddrChainCombs = append(singleSideAddrChainCombs, genKey(v.ChainA, v.ChannelA, v.ChainAAddress, v.PairId))
		} else {
			allAddrChainCombs = append(allAddrChainCombs, genKey(v.ChainB, v.ChannelB, v.ChainBAddress, v.PairId))
		}
	}

	if len(singleSideAddrChainCombs) == 0 {
		return pairInfoList, false, nil
	}

	matchedPairIdMap := make(map[string]struct{})
	for _, sc := range singleSideAddrChainCombs {
		chain1, channel1, address1, pairId1 := splitKey(sc)
		for _, ac := range allAddrChainCombs {
			chain2, _, address2, _ := splitKey(ac)
			if chain1 == chain2 {
				continue
			}
			tempPairList, matched, err := repository.GetChannelPairInfoByAddressPair(chain1, address1, chain2, address2)
			if err != nil {
				return nil, false, err
			}

			if matched {
				for _, t := range tempPairList {
					if (t.ChainA == chain1 && t.ChainAAddress == address1 && t.ChannelA == channel1) ||
						(t.ChainB == chain1 && t.ChainBAddress == address1 && t.ChannelB == channel1) {

						if _, ok := pairIdMap[t.PairId]; !ok {
							pairInfoList = append(pairInfoList, t)
							pairIdMap[t.PairId] = struct{}{}
						}

						matchedPairIdMap[pairId1] = struct{}{}
					}
				}
			}
		}
	}

	if len(matchedPairIdMap) == 0 {
		return pairInfoList, false, nil
	}

	var newPairInfoList []entity.ChannelPairInfo
	for _, v := range pairInfoList {
		if _, ok := matchedPairIdMap[v.PairId]; !ok {
			newPairInfoList = append(newPairInfoList, v)
		}
	}

	return newPairInfoList, true, nil
}

func matchRelayerChannelPairInfo(pairInfoList []entity.ChannelPairInfo) ([]entity.ChannelPairInfo, bool, error) {
	pairIds := getRelayerPairIds(pairInfoList)
	matchedPairInfoList := make([]entity.ChannelPairInfo, 0, len(pairInfoList))
	for _, val := range pairInfoList {
		pairInfos, _, err := repository.GetChannelPairInfoByAddressPair(val.ChainA, val.ChainAAddress, val.ChainB, val.ChainBAddress)
		if err != nil {
			logrus.Error("GetChannelPairInfoByAddressPair fail, "+err.Error(),
				" chainA:", val.ChainA, " chainB:", val.ChainB, " chainAAddr:", val.ChainAAddress, " chainBAddr:", val.ChainBAddress)
			return nil, false, err
		}
		matchedPairInfoList = append(matchedPairInfoList, pairInfos...)
	}

	newPairInfoList := make([]entity.ChannelPairInfo, 0, len(pairInfoList))
	for _, val := range matchedPairInfoList {
		if !utils.InArray(pairIds, val.PairId) {
			newPairInfoList = append(newPairInfoList, val)
			pairIds = append(pairIds, val.PairId)
		}
	}

	if len(newPairInfoList) == 0 {
		return pairInfoList, false, nil
	}

	pairInfoList = append(pairInfoList, newPairInfoList...)
	pairInfoList = removeEmptyChannelData(pairInfoList)
	return pairInfoList, true, nil
}

func getRelayerStatisticData(denomPriceMap map[string]dto.CoinItem, data *entity.IBCRelayerNew) *entity.IBCRelayerNew {
	wg := sync.WaitGroup{}
	wg.Add(2)
	var (
		totalFeeValue                      decimal.Decimal
		totalTxsValue                      decimal.Decimal
		relayedTotalTxs, relayedSuccessTxs int64
	)
	go func() {
		defer wg.Done()
		relayedFeeTotalTxs := int64(0)
		relayerFeeAmt := AggrRelayerFeeAmt(data)
		totalFeeValue = caculateRelayerTotalValue(denomPriceMap, relayerFeeAmt)
		txsItem := make([]vo.DenomFeeItem, 0, len(relayerFeeAmt))
		for _, val := range relayerFeeAmt {
			relayedFeeTotalTxs += val.Txs
			txsItem = append(txsItem, vo.DenomFeeItem{
				Denom:      val.Denom,
				DenomChain: val.Chain,
				Txs:        val.Txs,
				FeeValue:   val.AmtValue.String(),
			})
		}
		res := vo.TotalFeeCostResp{
			TotalTxs:        relayedFeeTotalTxs,
			TotalFeeValue:   totalFeeValue.String(),
			TotalDenomCount: int64(len(txsItem)),
			DenomList:       txsItem,
		}
		_ = relayerDataCache.SetTotalFeeCost(data.RelayerId, &res)
	}()

	go func() {
		defer wg.Done()
		relayerTxsAmt := AggrRelayerTxsAndAmt(data)
		txsItem := make([]vo.DenomTxsItem, 0, len(relayerTxsAmt))
		totalTxsValue = caculateRelayerTotalValue(denomPriceMap, relayerTxsAmt)
		for _, val := range relayerTxsAmt {
			txsItem = append(txsItem, vo.DenomTxsItem{
				BaseDenom:      val.Denom,
				BaseDenomChain: val.Chain,
				Txs:            val.Txs,
				TxsValue:       val.AmtValue.String(),
			})
			relayedTotalTxs += val.Txs
			relayedSuccessTxs += val.TxsSuccess
		}

		res := vo.TotalRelayedValueResp{
			TotalTxs:        relayedTotalTxs,
			TotalTxsValue:   totalTxsValue.String(),
			TotalDenomCount: int64(len(txsItem)),
			DenomList:       txsItem,
		}
		_ = relayerDataCache.SetTotalRelayedValue(data.RelayerId, &res)

	}()
	wg.Wait()
	data.RelayedTotalTxsValue = totalTxsValue.String()
	data.TotalFeeValue = totalFeeValue.String()
	data.RelayedTotalTxs = relayedTotalTxs
	data.RelayedSuccessTxs = relayedSuccessTxs
	return data
}

func getRelayerPairIds(relayerChannelPairInfo []entity.ChannelPairInfo) []string {
	pairIds := make([]string, 0, len(relayerChannelPairInfo))
	for i := range relayerChannelPairInfo {
		val := relayerChannelPairInfo[i]
		if val.PairId != "" {
			pairIds = append(pairIds, val.PairId)
		}
	}
	return pairIds
}

func removeEmptyChannelData(addrPairInfo []entity.ChannelPairInfo) []entity.ChannelPairInfo {
	emptyChannelData := make([]entity.ChannelPairInfo, 0, len(addrPairInfo))
	for i := range addrPairInfo {
		if addrPairInfo[i].ChannelA == "" || addrPairInfo[i].ChannelB == "" {
			emptyChannelData = append(emptyChannelData, addrPairInfo[i])
		}
	}
	if len(emptyChannelData) > 0 {
		dataMap := make(map[string]struct{}, len(addrPairInfo))
		channelPairArrs := make([]entity.ChannelPairInfo, 0, len(addrPairInfo))
		for _, val := range addrPairInfo {
			if val.ChannelB != "" && val.ChannelA != "" {
				key := entity.GenerateDistRelayerId(val.ChainA, val.ChainAAddress, val.ChainB, val.ChainBAddress)
				dataMap[key] = struct{}{}
				channelPairArrs = append(channelPairArrs, val)
			}
		}

		for _, val := range emptyChannelData {
			key := entity.GenerateDistRelayerId(val.ChainA, val.ChainAAddress, val.ChainB, val.ChainBAddress)
			if _, ok := dataMap[key]; !ok {
				channelPairArrs = append(channelPairArrs, val)
			}
		}
		return channelPairArrs
	}
	return addrPairInfo
}
