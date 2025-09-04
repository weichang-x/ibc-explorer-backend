package task

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/qiniu/qmgo"
	"go.mongodb.org/mongo-driver/mongo"

	"time"

	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/vo"
	"github.com/irisnet/ibc-explorer-backend/internal/app/pkg/lcd"
	"github.com/irisnet/ibc-explorer-backend/internal/app/repository"
	"github.com/irisnet/ibc-explorer-backend/internal/app/utils"
	"github.com/sirupsen/logrus"
)

type IbcChainConfigTask struct {
	allChainList    []string  // all chain list
	channelStateMap *sync.Map // channel -> state map
	chainUpdateMap  *sync.Map // map[string]bool chain
	chainChannelMap *sync.Map
	chainIdNameMap  map[string]string
}

var _ibcChainConfigTask Task = new(IbcChainConfigTask)

func (t *IbcChainConfigTask) Name() string {
	return "ibc_chain_config_task"
}
func (t *IbcChainConfigTask) Cron() int {
	if taskConf.CronTimeChainConfigTask > 0 {
		return taskConf.CronTimeChainConfigTask
	}
	return EveryMinute
}

func (t *IbcChainConfigTask) Run() int {
	t.init()
	chainConfList, err := t.getChainConf()
	if err != nil {
		logrus.Errorf("task %s getChainConf error, %s", t.Name(), err.Error())
		return -1
	}

	var wg sync.WaitGroup
	wg.Add(len(chainConfList))
	for i := range chainConfList {
		go func() {
			defer wg.Done()
			channelPathList, err := t.getIbcChannels(chainConfList[i].ChainName, chainConfList[i].GrpcRestGateway, chainConfList[i].LcdApiPath.ChannelsPath)
			if err != nil || len(channelPathList) == 0 {
				t.chainUpdateMap.Store(chainConfList[i].ChainName, false)
			} else {
				t.setChainAndCounterpartyState(chainConfList[i], channelPathList)
				t.chainUpdateMap.Store(chainConfList[i].ChainName, true)
			}
			if len(channelPathList) > 0 {
				t.chainChannelMap.Store(chainConfList[i].ChainName, channelPathList)
			}
		}()
	}
	wg.Wait()

	for i := range chainConfList {
		t.setCounterpartyState(chainConfList[i].ChainName)
	}

	bulk := chainConfigRepo.Bulk()
	var change bool
	for i := range chainConfList {
		enableUpdate, ok := t.chainUpdateMap.Load(chainConfList[i].ChainName)
		if ok {
			if enableUpdate.(bool) {
				change = true
				t.updateChain(bulk, chainConfList[i])
			}
		}
	}
	if change {
		bulk.SetOrdered(false)
		if _, err := bulk.Run(context.Background()); err != nil && err != mongo.ErrNoDocuments {
			logrus.Errorf("task %s bulk UpdateIbcInfo error, %s", t.Name(), err.Error())
		}
	}

	return 1
}

func (t *IbcChainConfigTask) init() {
	t.channelStateMap = new(sync.Map)
	t.chainUpdateMap = new(sync.Map)
	t.chainChannelMap = new(sync.Map)
	mapData, err := repository.GetChainIdNameMap()
	if err != nil {
		logrus.Fatal(err.Error())
	}
	t.chainIdNameMap = mapData
}

func (t *IbcChainConfigTask) getChainConf() ([]*entity.ChainConfig, error) {
	chainConfList, err := chainConfigRepo.FindAll()
	if err != nil {
		return nil, err
	}

	allChainList := make([]string, 0, len(chainConfList))
	for i := range chainConfList {
		allChainList = append(allChainList, chainConfList[i].ChainName)
	}
	t.allChainList = allChainList

	return chainConfList, nil
}

func (t *IbcChainConfigTask) getIbcChannels(chain, lcd, originApiPath string) ([]*entity.ChannelPath, error) {
	if lcd == "" {
		logrus.Errorf("task %s %s getIbcChannels error, lcd error", t.Name(), chain)
		return nil, fmt.Errorf("lcd error")
	}

	limit := 1000
	offset := 0
	var channelPathList []*entity.ChannelPath

	for {
		apiPath := strings.ReplaceAll(originApiPath, replaceHolderOffset, strconv.Itoa(offset))
		apiPath = strings.ReplaceAll(apiPath, replaceHolderLimit, strconv.Itoa(limit))
		url := fmt.Sprintf("%s%s", lcd, apiPath)
		bz, err := utils.HttpGet(url)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			logrus.Errorf("task %s %s getIbcChannels error, %v", t.Name(), chain, err)
			return nil, err
		}

		var resp vo.IbcChannelsResp
		err = json.Unmarshal(bz, &resp)
		if err != nil {
			logrus.Errorf("task %s %s getIbcChannels error, %v", t.Name(), chain, err)
			return nil, err
		}

		for _, v := range resp.Channels {
			if v.PortId == "icahost" || strings.HasPrefix(v.PortId, "icacontroller-") {
				continue
			}
			channelPathList = append(channelPathList, &entity.ChannelPath{
				State:     v.State,
				PortId:    v.PortId,
				ChannelId: v.ChannelId,
				Chain:     "",
				ScChain:   chain,
				Counterparty: entity.CounterParty{
					State:     "",
					PortId:    v.Counterparty.PortId,
					ChannelId: v.Counterparty.ChannelId,
				},
			})
			k := fmt.Sprintf("%s%s%s%s%s", chain, v.PortId, v.ChannelId, v.Counterparty.PortId, v.Counterparty.ChannelId)
			t.channelStateMap.Store(k, v.State)
		}

		if len(resp.Channels) < limit {
			break
		}
		offset += limit
	}

	return channelPathList, nil
}

func (t *IbcChainConfigTask) setChainAndCounterpartyState(chain *entity.ChainConfig, channelPathList []*entity.ChannelPath) {
	existChannelStateMap := make(map[string]*entity.ChannelPath)
	for _, ibcInfo := range chain.IbcInfo {
		for _, path := range ibcInfo.Paths {
			key := fmt.Sprintf("%s%s%s%s", path.PortId, path.ChannelId, path.Counterparty.PortId, path.Counterparty.ChannelId)
			existChannelStateMap[key] = path
		}
	}

	lcdConnectionErr := false
	for _, v := range channelPathList {
		key := fmt.Sprintf("%s%s%s%s", v.PortId, v.ChannelId, v.Counterparty.PortId, v.Counterparty.ChannelId)
		existChannelState, ok := existChannelStateMap[key]
		if ok {
			v.Counterparty.State = existChannelState.Counterparty.State
		}

		if ok && existChannelState.Chain != "" && existChannelState.ClientId != "" {
			v.Chain = existChannelState.Chain
			v.ClientId = existChannelState.ClientId
		} else {
			if !lcdConnectionErr {
				stateResp, err := lcd.QueryClientState(chain.GrpcRestGateway, chain.LcdApiPath.ClientStatePath, v.PortId, v.ChannelId)
				if err != nil {
					lcdConnectionErr = isConnectionErr(err)
					logrus.Errorf("task %s %s queryClientState error, %v", t.Name(), chain.ChainName, err)
				} else {
					v.Chain = t.chainIdNameMap[stateResp.IdentifiedClientState.ClientState.ChainId]
					v.ClientId = stateResp.IdentifiedClientState.ClientId
				}
			}
		}
	}
}

func (t *IbcChainConfigTask) setCounterpartyState(chain string) {
	channels, ok := t.chainChannelMap.Load(chain)
	if !ok {
		return
	}

	for _, v := range channels.([]*entity.ChannelPath) {
		key := fmt.Sprintf("%s%s%s%s%s", v.Chain, v.Counterparty.PortId, v.Counterparty.ChannelId, v.PortId, v.ChannelId)
		counterpartyState, ok := t.channelStateMap.Load(key)
		if ok {
			v.Counterparty.State = counterpartyState.(string)
		}
	}
}

func (t *IbcChainConfigTask) updateChain(bulk *qmgo.Bulk, chainConf *entity.ChainConfig) {
	channelGroupMap := make(map[string][]*entity.ChannelPath)
	channels, ok := t.chainChannelMap.Load(chainConf.ChainName)
	if !ok {
		return
	}

	for _, v := range channels.([]*entity.ChannelPath) {
		if !utils.InArray(t.allChainList, v.Chain) {
			continue
		}

		channelGroupMap[v.Chain] = append(channelGroupMap[v.Chain], v)
	}

	ibcInfoList := make([]*entity.IbcInfo, 0, len(channelGroupMap))
	for dcChain, paths := range channelGroupMap {
		sort.Slice(paths, func(i, j int) bool {
			return paths[i].ChannelId < paths[j].ChannelId
		})
		ibcInfoList = append(ibcInfoList, &entity.IbcInfo{
			Chain: dcChain,
			Paths: paths,
		})
	}

	sort.Slice(ibcInfoList, func(i, j int) bool {
		return ibcInfoList[i].Chain < ibcInfoList[i].Chain
	})

	hashCode := utils.Md5(utils.MustMarshalJsonToStr(ibcInfoList))
	if hashCode == chainConf.IbcInfoHashLcd {
		return
	}

	chainConf.IbcInfoHashLcd = hashCode
	chainConf.IbcInfo = ibcInfoList
	if err := chainConfigRepo.UpdateIbcInfo(bulk, chainConf); err != nil {
		logrus.Errorf("task %s %s UpdateIbcInfo error, %v", t.Name(), chainConf.ChainName, err)
	}
}
