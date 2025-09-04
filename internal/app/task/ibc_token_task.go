package task

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/irisnet/ibc-explorer-backend/internal/app/pkg/ibctool"
	"github.com/irisnet/ibc-explorer-backend/internal/app/repository/cache"
	"github.com/qiniu/qmgo"
	"go.mongodb.org/mongo-driver/bson"

	v8 "github.com/go-redis/redis/v8"
	"github.com/irisnet/ibc-explorer-backend/internal/app/constant"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/dto"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/vo"
	"github.com/irisnet/ibc-explorer-backend/internal/app/utils"
	"github.com/irisnet/ibc-explorer-backend/internal/app/utils/bech32"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

type TokenTask struct {
	chains           []string
	chainLcdMap      map[string]string
	chainLcdApiMap   map[string]entity.ApiPath
	escrowAddressMap map[string][]string
	baseDenomList    entity.AuthDenomList
	ibcReceiveTxsMap map[string]int64
}

func (t *TokenTask) Name() string {
	return "ibc_token_task"
}

func (t *TokenTask) Cron() int {
	return ThreeMinute
}

func (t *TokenTask) Run() int {
	err := t.analyzeChainConf()
	if err != nil {
		logrus.Errorf("task %s run error, %v", t.Name(), err)
		return -1
	}

	if err = t.initDenomData(); err != nil {
		return -1
	}

	_ = t.todayStatistics()

	_ = t.yesterdayStatistics()

	existedTokenList, newTokenList, removedTokenList, err := t.getAllToken()
	if err != nil {
		return -1
	}

	t.setTokenType(existedTokenList)

	_ = t.setTokenPrice(existedTokenList, newTokenList)

	_ = t.setDenomSupply(existedTokenList, newTokenList)

	_ = t.setIbcTransferTxs(existedTokenList, newTokenList)

	_ = t.setIbcTransferAmount(existedTokenList, newTokenList)

	if err = t.ibcReceiveTxs(); err != nil {
		return -1
	}

	t.calculateTokenStatistics(existedTokenList, newTokenList, removedTokenList)

	if err = tokenRepo.InsertBatch(newTokenList); err != nil {
		logrus.Errorf("task %s insert new tokens error, %v", t.Name(), err)
	}

	for _, v := range removedTokenList {
		if err = tokenRepo.Delete(v.BaseDenom, v.Chain); err != nil {
			logrus.Errorf("task %s delete removed tokens error, %v", t.Name(), err)
		}
	}
	if length := len(existedTokenList); length > 1000 {
		batchSize := constant.DefaultLimit
		for i := 0; i < length; i += batchSize {
			end := i + batchSize
			if end > length {
				end = length
			}
			batch := existedTokenList[i:end]
			err := bulkAppendAndRun(entity.IBCToken{}.CollectionName(), func(bulk *qmgo.Bulk) {
				for _, v := range batch {
					tokenRepo.UpdateToken(bulk, v)
				}
			})
			if err != nil && err != mongo.ErrNoDocuments {
				logrus.Errorf("task %s batch bulk update token error, %s", t.Name(), err.Error())
			}
			logrus.Infof("task %s batch bulk update token index:%d end:%d  total:%d", t.Name(), i, end, length)
		}
	} else {
		bulk := tokenRepo.Bulk()
		for _, v := range existedTokenList {
			tokenRepo.UpdateToken(bulk, v)
		}
		bulk.SetOrdered(false)
		if _, err := bulk.Run(context.Background()); err != nil {
			logrus.Errorf("task %s  bulk update token fail, err:%s", t.Name(), err.Error())
		}
	}

	t.updateIBCChain()
	return 1
}

func (t *TokenTask) initDenomData() error {
	baseDenomList, err := authDenomRepo.FindAll()
	if err != nil {
		logrus.Errorf("task %s authDenomRepo.FindAll error, %v", t.Name(), err)
		return err
	}
	t.baseDenomList = baseDenomList
	return nil
}

// getAllToken Cannot automatically delete nonexistent tokens
func (t *TokenTask) getAllToken() (entity.IBCTokenList, entity.IBCTokenList, entity.IBCTokenList, error) {
	allTokenList, err := denomRepo.FindBaseDenom()
	if err != nil {
		logrus.Errorf("task %s run error, %v", t.Name(), err)
		return nil, nil, nil, err
	}

	existedTokenList, err := tokenRepo.FindAll()
	if err != nil {
		logrus.Errorf("task %s run error, %v", t.Name(), err)
		return nil, nil, nil, err
	}

	existedTokenMap := existedTokenList.ConvertToMap()
	var newTokenList, stillExistedTokenList entity.IBCTokenList

	for _, v := range allTokenList {
		token, ok := existedTokenMap[fmt.Sprintf("%s%s", v.Chain, v.BaseDenom)]
		if ok {
			stillExistedTokenList = append(stillExistedTokenList, token)
			continue
		}

		newTokenList = append(newTokenList, &entity.IBCToken{
			BaseDenom:      v.BaseDenom,
			Chain:          v.Chain,
			Type:           t.tokenType(v.BaseDenom, v.Chain),
			Price:          constant.UnknownTokenPrice,
			Currency:       constant.DefaultCurrency,
			Supply:         constant.UnknownDenomAmount,
			TransferAmount: constant.UnknownDenomAmount,
			TransferTxs:    0,
			ChainsInvolved: 1,
		})
	}

	var removedTokenList entity.IBCTokenList
	allTokenMap := allTokenList.ConvertToMap()
	for i := range existedTokenList {
		_, ok := allTokenMap[fmt.Sprintf("%s%s", existedTokenList[i].Chain, existedTokenList[i].BaseDenom)]
		if !ok {
			removedTokenList = append(removedTokenList, existedTokenList[i])
		}
	}

	return existedTokenList, newTokenList, removedTokenList, nil
}

func (t *TokenTask) tokenType(baseDenom, chain string) entity.TokenType {
	for _, v := range t.baseDenomList {
		if v.Chain == chain && v.Denom == baseDenom {
			return entity.TokenTypeAuthed
		}
	}

	return entity.TokenTypeOther
}

func (t *TokenTask) setTokenType(existedTokenList entity.IBCTokenList) {
	for i := range existedTokenList {
		existedTokenList[i].Type = t.tokenType(existedTokenList[i].BaseDenom, existedTokenList[i].Chain)
	}
}

func (t *TokenTask) setTokenPrice(existedTokenList, newTokenList entity.IBCTokenList) error {
	tokenPriceMap, err := tokenPriceRepo.GetAll()
	if err != nil {
		logrus.Errorf("task %s `setTokenPrice` error, %v", t.Name(), err)
		return err
	}

	baseDenomMap := t.baseDenomList.ConvertToMap()
	setPrice := func(tokenList entity.IBCTokenList, tokenPriceMap map[string]float64) {
		for i := range tokenList {
			denom, ok := baseDenomMap[fmt.Sprintf("%s%s", tokenList[i].Chain, tokenList[i].BaseDenom)]
			if !ok || denom.CoinId == "" {
				continue
			}

			price, ok := tokenPriceMap[denom.CoinId]
			if ok {
				tokenList[i].Price = price
			}
		}
	}

	setPrice(existedTokenList, tokenPriceMap)
	setPrice(newTokenList, tokenPriceMap)
	return nil
}

func (t *TokenTask) analyzeChainConf() error {
	configList, err := chainConfigRepo.FindAll()
	if err != nil {
		logrus.Errorf("task %s analyzeChainConf error, %v", t.Name(), err)
		return err
	}

	chains := make([]string, 0, len(configList))
	chainLcdMap := make(map[string]string)
	chainLcdApiMap := make(map[string]entity.ApiPath)
	escrowAddressMap := make(map[string][]string)
	for i := range configList {
		chains = append(chains, configList[i].ChainName)
		chainLcdMap[configList[i].ChainName] = configList[i].GrpcRestGateway
		chainLcdApiMap[configList[i].ChainName] = configList[i].LcdApiPath
		address, err := t.analyzeChainEscrowAddress(configList[i].IbcInfo, configList[i].AddrPrefix)
		if err != nil {
			continue
		}
		escrowAddressMap[configList[i].ChainName] = address
	}
	t.chains = chains
	t.chainLcdMap = chainLcdMap
	t.chainLcdApiMap = chainLcdApiMap
	t.escrowAddressMap = escrowAddressMap
	return nil
}

func (t *TokenTask) analyzeChainEscrowAddress(info []*entity.IbcInfo, addrPrefix string) ([]string, error) {
	var addrList []string
	for _, v := range info {
		for _, p := range v.Paths {
			address, err := t.getEscrowAddress(p.PortId, p.ChannelId, addrPrefix)
			if err != nil {
				continue
			}
			addrList = append(addrList, address)
		}
	}
	return addrList, nil
}

func (t *TokenTask) getEscrowAddress(portID, channelID, addrPrefix string) (string, error) {
	contents := fmt.Sprintf("%s/%s", portID, channelID)
	const version = "ics20-1"
	preImage := []byte(version)
	preImage = append(preImage, 0)
	preImage = append(preImage, contents...)
	hash := sha256.Sum256(preImage)

	addr, err := bech32.ConvertAndEncode(addrPrefix, hash[:20])
	if err != nil {
		logrus.Errorf("task %s getEscrowAddress error, %v", t.Name(), err)
		return "", err
	}

	return addr, nil
}

func (t *TokenTask) setDenomSupply(existedTokenList, newTokenList entity.IBCTokenList) error {

	setSupply := func(list entity.IBCTokenList) {
		for i := range list {
			if utils.InArray(t.chains, list[i].Chain) {
				supply, err := denomDataRepo.GetSupply(list[i].Chain, list[i].BaseDenom)
				if err == nil {
					list[i].Supply = supply
				}
			}
		}
	}

	setSupply(existedTokenList)
	setSupply(newTokenList)
	return nil
}

func (t *TokenTask) setIbcTransferTxs(existedTokenList, newTokenList entity.IBCTokenList) error {
	st := time.Now().Unix()
	txsCount, err := chainOutflowStatisticsRepo.AggrDenomTxs()
	et := time.Now().Unix()
	if et-st > 10 {
		logrus.Warningf("task %s chainOutflowStatisticsRepo.AggrDenomTxs slow(%d s)", t.Name(), et-st)
	}

	if err != nil {
		logrus.Errorf("task %s setIbcTransferTxs error, %v", t.Name(), err)
		return err
	}

	setTxs := func(tokenList entity.IBCTokenList, txsCount []*dto.AggrDenomTxsDTO) {
		for i := range tokenList {
			var count int64
			for _, tx := range txsCount {
				if tx.BaseDenom == tokenList[i].BaseDenom && tx.BaseDenomChain == tokenList[i].Chain {
					count += tx.TxsNumber
				}
			}

			tokenList[i].TransferTxs = count
		}
	}

	setTxs(existedTokenList, txsCount)
	setTxs(newTokenList, txsCount)
	return nil
}

func (t *TokenTask) setIbcTransferAmount(existedTokenList, newTokenList entity.IBCTokenList) error {
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(t.chains))
	for _, v := range t.chains {
		go func(c string, addrs []string) {
			defer waitGroup.Done()
			t.getTransAmountFromLcd(c, addrs)
		}(v, t.escrowAddressMap[v])
	}
	waitGroup.Wait()

	setTransAmount := func(list entity.IBCTokenList) {
		for i := range list {
			if utils.InArray(t.chains, list[i].Chain) {
				amount, err := denomDataRepo.GetTransferAmount(list[i].Chain, list[i].BaseDenom)
				if err != nil {
					if err == v8.Nil {
						list[i].TransferAmount = constant.ZeroDenomAmount
					}
					continue
				}
				list[i].TransferAmount = amount
			}
		}
	}

	setTransAmount(existedTokenList)
	setTransAmount(newTokenList)
	return nil
}

func (t *TokenTask) getTransAmountFromLcd(chain string, addrList []string) {
	denomTransAmountMap := make(map[string]decimal.Decimal)
	lcd := t.chainLcdMap[chain]
	apiPath := t.chainLcdApiMap[chain].BalancesPath
	for _, addr := range addrList {
		limit := 500
		key := ""
		earlyTermination := false
		baseUrl := strings.ReplaceAll(fmt.Sprintf("%s%s", lcd, apiPath), entity.ApiBalancesPathPlaceholder, addr)

		for {
			var url string
			if key == "" {
				url = fmt.Sprintf("%s?pagination.limit=%d", baseUrl, limit)
			} else {
				url = fmt.Sprintf("%s?pagination.limit=%d&pagination.key=%s", baseUrl, limit, key)
			}

			bz, err := utils.HttpGet(url)
			if err != nil {
				if isConnectionErr(err) {
					earlyTermination = true
				}
				logrus.Errorf("task %s chain: %s getTransAmountFromLcd error, %v", t.Name(), chain, err)
				break
			}

			var balancesResp vo.BalancesResp
			err = json.Unmarshal(bz, &balancesResp)
			if err != nil {
				logrus.Errorf("task %s chain: %s getTransAmountFromLcd error, %v", t.Name(), chain, err)
				break
			}

			for _, v := range balancesResp.Balances {
				amount, err := decimal.NewFromString(v.Amount)
				if err != nil {
					logrus.Errorf("task %s chain: %s getTransAmountFromLcd error, %v", t.Name(), chain, err)
					continue
				}

				d, ok := denomTransAmountMap[v.Denom]
				if !ok {
					denomTransAmountMap[v.Denom] = amount
				} else {
					denomTransAmountMap[v.Denom] = d.Add(amount)
				}
			}

			if balancesResp.Pagination.NextKey == nil {
				break
			} else {
				key = *balancesResp.Pagination.NextKey
			}
		}

		if earlyTermination {
			break
		}
	}

	if len(denomTransAmountMap) > 0 {
		_, _ = denomDataRepo.DelTransferAmount(chain)
		denomTransAmountStrMap := make(map[string]string)
		for k, v := range denomTransAmountMap {
			denomTransAmountStrMap[k] = v.String()
		}
		if err := denomDataRepo.SetTransferAmount(chain, denomTransAmountStrMap); err != nil {
			logrus.Errorf("task %s denomDataRepo.SetTransferAmount error, %v", t.Name(), err)
		}
	}
}

func (t *TokenTask) getTokenScale(baseDenom, chain string) int {
	var scale int
	for _, v := range t.baseDenomList {
		if v.Chain == chain && v.Denom == baseDenom {
			scale = v.Scale
			break
		}
	}

	return scale
}

func (t *TokenTask) calculateTokenStatistics(existedTokenList, newTokenList, removedTokenList entity.IBCTokenList) {
	for i := range removedTokenList {
		t.ibcTokenTraceRemove(removedTokenList[i])
	}

	for i := range existedTokenList {
		chainNum, err := t.ibcTokenStatistics(existedTokenList[i])
		if err != nil {
			continue
		}
		existedTokenList[i].ChainsInvolved = chainNum
	}

	for i := range newTokenList {
		chainNum, err := t.ibcTokenStatistics(newTokenList[i])
		if err != nil {
			continue
		}
		newTokenList[i].ChainsInvolved = chainNum
	}
}

func (t *TokenTask) ibcTokenTraceRemove(token *entity.IBCToken) {
	if err := tokenTraceRepo.DelByBaseDenom(token.BaseDenom, token.Chain); err != nil {
		logrus.Errorf("task %s tokenTraceRepo.DelByBaseDenom error, %v", t.Name(), err)
	}
}

func (t *TokenTask) ibcTokenStatistics(ibcToken *entity.IBCToken) (int64, error) {
	denomList, err := denomRepo.FindByBaseDenom(ibcToken.BaseDenom, ibcToken.Chain)
	if err != nil {
		logrus.Errorf("task %s FindByBaseDenom error, %v", t.Name(), err)
		return 0, nil
	}
	priceMap := cache.TokenPriceMap()
	scale := t.getTokenScale(ibcToken.BaseDenom, ibcToken.Chain)
	price := ibcToken.Price
	allTokenStatisticsList := make([]*entity.IBCTokenTrace, 0, len(denomList))
	chainsSet := utils.NewStringSet()
	for _, v := range denomList {
		denomType := t.ibcTokenStatisticsType(ibcToken.BaseDenom, v)
		var denomAmount, denomSupply string
		if denomType == entity.TokenTraceTypeGenesis {
			denomSupply = ibcToken.Supply
			denomAmount = t.ibcDenomAmountGenesis(ibcToken.Supply, ibcToken.TransferAmount)
		} else {
			denomSupply = t.ibcDenomSupply(v.Chain, v.Denom)
			denomAmount = t.ibcDenomAmount(v.Chain, v.Denom, denomSupply)
		}

		if denomSupply == constant.ZeroDenomAmount {
			continue
		}
		if v.RootDenom != "" && v.RootDenom != ibcToken.BaseDenom {
			key := fmt.Sprintf("%s%s", v.Denom, v.Chain)
			coin := priceMap[key]
			price = coin.Price
			scale = coin.Scale
		}

		allTokenStatisticsList = append(allTokenStatisticsList, &entity.IBCTokenTrace{
			Denom:          v.Denom,
			Chain:          v.Chain,
			DenomPath:      v.DenomPath,
			BaseDenom:      ibcToken.BaseDenom,
			BaseDenomChain: ibcToken.Chain,
			Type:           denomType,
			IBCHops:        v.IBCHops,
			DenomSupply:    denomSupply,
			DenomAmount:    denomAmount,
			DenomValue:     t.ibcDenomValue(denomAmount, price, scale).Round(constant.DefaultValuePrecision).String(),
			ReceiveTxs:     t.ibcReceiveTxsMap[fmt.Sprintf("%s%s", v.Denom, v.Chain)],
		})

		chainsSet.Add(v.Chain)
	}

	err = tokenTraceRepo.BatchSwap(allTokenStatisticsList, ibcToken.BaseDenom, ibcToken.Chain)
	if err != nil {
		logrus.Errorf("task %s BatchSwap error,base denom:%s, %v", t.Name(), ibcToken.BaseDenom, err)
		return 0, err
	}
	return int64(len(chainsSet)), nil
}

func (t *TokenTask) ibcReceiveTxs() error {
	var txsMap = make(map[string]int64)

	aggr, err := tokenTraceStatisticsRepo.Aggr()
	if err != nil {
		logrus.Errorf("task %s ibcReceiveTxs error, %v", t.Name(), err)
		return err
	}

	for _, v := range aggr {
		txsMap[fmt.Sprintf("%s%s", v.Denom, v.Chain)] = v.ReceiveTxs
	}

	t.ibcReceiveTxsMap = txsMap
	return nil
}

func (t *TokenTask) ibcDenomValue(amount string, price float64, scale int) decimal.Decimal {
	if amount == constant.UnknownDenomAmount || amount == constant.ZeroDenomAmount || price == 0 || price == constant.UnknownTokenPrice {
		return decimal.Zero
	}

	amountDecimal, err := decimal.NewFromString(amount)
	if err != nil {
		logrus.Errorf("task %s ibcDenomValue error, %v", t.Name(), err)
		return decimal.Zero
	}

	value := amountDecimal.Div(decimal.NewFromFloat(math.Pow10(scale))).
		Mul(decimal.NewFromFloat(price))

	return value
}

func (t *TokenTask) ibcDenomSupply(chain, denom string) string {
	supplyAmount, err := denomDataRepo.GetSupply(chain, denom)
	if err != nil {
		if err == v8.Nil {
			return constant.ZeroDenomAmount
		}
		return constant.UnknownDenomAmount
	}

	return supplyAmount
}

func (t *TokenTask) ibcDenomAmount(chain, denom, supply string) string {
	transferAmount, err := denomDataRepo.GetTransferAmount(chain, denom)
	if err != nil {
		transferAmount = constant.ZeroDenomAmount
	}

	sd, _ := decimal.NewFromString(supply)
	td, _ := decimal.NewFromString(transferAmount)
	if sd.GreaterThanOrEqual(td) {
		return sd.Sub(td).String()
	}

	return constant.UnknownDenomAmount
}

func (t *TokenTask) ibcDenomAmountGenesis(supply, transAmount string) string {
	if supply == constant.UnknownDenomAmount || transAmount == constant.UnknownDenomAmount {
		return constant.UnknownDenomAmount
	}

	sd, err := decimal.NewFromString(supply)
	if err != nil {
		return constant.UnknownDenomAmount
	}

	td, err := decimal.NewFromString(transAmount)
	if err != nil {
		return constant.UnknownDenomAmount
	}

	if sd.GreaterThanOrEqual(td) {
		return sd.Sub(td).String()
	}

	return constant.UnknownDenomAmount
}

func (t *TokenTask) ibcTokenStatisticsType(baseDenom string, denom *entity.IBCDenom) entity.TokenTraceType {
	if baseDenom == denom.Denom {
		return entity.TokenTraceTypeGenesis
	}

	for _, v := range t.baseDenomList {
		if v.Denom == denom.BaseDenom && v.Chain == denom.BaseDenomChain {
			return entity.TokenTraceTypeAuthed
		}
	}

	return entity.TokenTraceTypeOther
}

func (t *TokenTask) updateIBCChain() {
	ibcChainList, err := tokenTraceRepo.AggregateIBCChain()
	if err != nil {
		logrus.Errorf("task %s updateIBCChain error, %v", t.Name(), err)
		return
	}
	bulk := chainRepo.Bulk()
	for _, v := range ibcChainList {
		vd := decimal.NewFromFloat(v.DenomValue).Round(constant.DefaultValuePrecision).String()
		chainRepo.UpdateIbcTokenValue(bulk, v.Chain, v.Count, vd)
	}
	bulk.SetOrdered(false)
	if _, err := bulk.Run(context.Background()); err != nil && err != mongo.ErrNoDocuments {
		logrus.Errorf("task %s bulk updateIBCChain error, %s", t.Name(), err.Error())
	}
	return
}

func (t *TokenTask) todayStatistics() error {
	logrus.Infof("task %s exec today statistics", t.Name())
	startTime, endTime := todayUnix()
	segments := []*segment{
		{
			StartTime: startTime,
			EndTime:   endTime,
		},
	}
	if err := TokenIncrementStatistics(segments); err != nil {
		logrus.Errorf("task %s todayStatistics error, %v", t.Name(), err)
		return err
	}

	return nil
}

func (t *TokenTask) yesterdayStatistics() error {
	ok, seg := whetherCheckYesterdayStatistics(t.Name(), t.Cron())
	if !ok {
		return nil
	}

	logrus.Infof("task %s check yeaterday statistics", t.Name())
	if err := TokenIncrementStatistics([]*segment{seg}); err != nil {
		logrus.Errorf("task %s todayStatistics error, %v", t.Name(), err)
		return err
	}

	return nil
}

func (t *TokenTask) CorrectTokenTrace() int {
	denomMap := make(map[string]string, 1024)
	createAt := int64(0)
	for {
		denoms, err := denomRepo.FindAll(createAt, constant.DefaultLimit)
		if err != nil {
			logrus.Errorf("task %s correct token to find all denom fail, err:%s", t.Name(), err.Error())
			return 0
		}

		for _, denom := range denoms {
			denomMap[denom.Chain+":"+denom.Denom] = denom.BaseDenom + ":" + denom.BaseDenomChain
		}

		createAt = denoms[len(denoms)-1].CreateAt
		if len(denoms) <= constant.DefaultLimit {
			break
		}
	}
	priceMap := cache.TokenPriceMap()
	skip, limit := int(0), int(1000)
	for {
		res, err := tokenTraceRepo.FindInvalidDenomValue(int64(skip), int64(limit))
		if err != nil {
			logrus.Errorf("task %s  token_trace find invalid denom_value fail, err:%s", t.Name(), err.Error())
			return -1
		}
		bulk := tokenTraceRepo.Bulk()
		for _, val := range res {
			if data, ok := denomMap[val.Chain+":"+val.Denom]; ok && len(strings.Split(data, ":")) == 2 {
				arr := strings.Split(data, ":")
				val.BaseDenom = arr[0]
				val.BaseDenomChain = arr[1]
			}
			txsValue := ibctool.CalculateDenomValue(priceMap, val.BaseDenom, val.BaseDenomChain, utils.AmountToDecimal(val.DenomValue))
			if !txsValue.IsZero() {
				val.DenomValue = txsValue.String()
				bulk.Upsert(bson.M{
					"chain": val.Chain,
					"denom": val.Denom,
				}, val)
			}
		}
		if len(res) > 0 {
			bulk.SetOrdered(false)
			if _, err := bulk.Run(context.Background()); err != nil {
				logrus.Errorf("task %s  token_trace update denom_value fail, err:%s", t.Name(), err.Error())
				return -1
			}
		}

		if len(res) < limit {
			break
		}
		skip += limit
	}
	return 1
}
