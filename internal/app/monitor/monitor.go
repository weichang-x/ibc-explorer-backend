package monitor

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/vo"
	"github.com/irisnet/ibc-explorer-backend/internal/app/repository"
	"github.com/irisnet/ibc-explorer-backend/internal/app/repository/cache"
	"github.com/irisnet/ibc-explorer-backend/internal/app/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	cronTaskStatusMetric  *prometheus.GaugeVec
	lcdConnectStatsMetric *prometheus.GaugeVec
	redisStatusMetric     prometheus.Gauge

	chainConfigRepo   repository.IChainConfigRepo   = new(repository.ChainConfigRepo)
	chainRegistryRepo repository.IChainRegistryRepo = new(repository.ChainRegistryRepo)
)

const (
	v1beta1        = "v1beta1"
	v1             = "v1"
	nodeInfo       = "/cosmos/base/tendermint/v1beta1/node_info"
	v1Channels     = "/ibc/core/channel/v1/channels"
	apiChannels    = "/ibc/core/channel/%s/channels?pagination.offset=OFFSET&pagination.limit=LIMIT&pagination.count_total=true"
	apiClientState = "/ibc/core/channel/%s/channels/CHANNEL/ports/PORT/client_state"
)

func SetCronTaskStatusMetricValue(taskName string, value float64) {
	if cronTaskStatusMetric != nil {
		cronTaskStatusMetric.With(prometheus.Labels{TagName: taskName}).Set(value)
	}
}

func lcdConnectionStatus(quit chan bool) {
	timeLong := time.Duration(120)
	t := time.NewTimer(timeLong * time.Second)
	for {
		now := time.Now()
		next := now.Add(timeLong)
		t.Reset(next.Sub(now))
		select {
		case <-t.C:
			chainCfgs, err := chainConfigRepo.FindAllOpenChainInfos()
			if err != nil {
				logrus.Error(err.Error())
				return
			}
			for i := range chainCfgs {
				if checkLcd(chainCfgs[i].GrpcRestGateway, chainCfgs[i].LcdApiPath.ChannelsPath) {
					lcdConnectStatsMetric.With(prometheus.Labels{ChainTag: chainCfgs[i].ChainName}).Set(float64(1))
				} else {
					if switchLcd(chainCfgs[i]) {
						lcdConnectStatsMetric.With(prometheus.Labels{ChainTag: chainCfgs[i].ChainName}).Set(float64(1))
					} else {
						lcdConnectStatsMetric.With(prometheus.Labels{ChainTag: chainCfgs[i].ChainName}).Set(float64(-1))
						logrus.Errorf("monitor chain %s lcd is unavailable", chainCfgs[i].ChainName)
					}
				}
			}

		case <-quit:
			logrus.Debug("quit signal recv  lcdConnectionStatus")
			return

		}
	}
}

func checkLcd(lcd string, channelPaths string) bool {
	channelPathUri := strings.Split(channelPaths, "?")[0]
	if _, err := utils.HttpGet(fmt.Sprintf("%s%s", lcd, channelPathUri)); err != nil {
		return false
	}

	return true
}

// checkAndUpdateLcd If lcd is ok, update db and return true. Else return false
func checkAndUpdateLcd(lcd string, cf *entity.ChainConfig) bool {
	if resp, err := utils.HttpGet(fmt.Sprintf("%s%s", lcd, nodeInfo)); err == nil {
		var data struct {
			NodeInfo struct {
				Network string `json:"network"`
			} `json:"default_node_info"`
		}
		_ = json.Unmarshal(resp, &data)
		if data.NodeInfo.Network == "" {
			logrus.Warnf("node api return data Unmarshal fail,api:%s", lcd+nodeInfo)
			return false
		}
		if data.NodeInfo.Network != cf.CurrentChainId {
			//return false, if lcd node_info network no match chain_id
			return false
		}

	} else {
		// return false,if lcd node_ifo api is not reach
		return false
	}

	var ok bool
	var version string
	if bz, err := utils.HttpGet(fmt.Sprintf("%s%s", lcd, v1Channels)); err == nil {
		var resp vo.IbcChannelsResp
		err = json.Unmarshal(bz, &resp)
		if err == nil {
			if resp.Pagination.Total > 0 {
				bz, err = utils.HttpGet(fmt.Sprintf("%s%s?pagination.limit=%d", lcd, v1Channels, resp.Pagination.Total))
				_ = json.Unmarshal(bz, &resp)
			}
			for _, v := range resp.Channels {
				if v.PortId != "icahost" && !strings.HasPrefix(v.PortId, "icacontroller-") {
					ok = true
					break
				}
			}
		} else {
			logrus.Warnf("channals api return data Unmarshal fail,api:%s", lcd+v1Channels)
		}
		version = v1
	} else if strings.Contains(err.Error(), "501 Not Implemented") {
		ok = true
		version = v1beta1
	} else {
		ok = false
	}

	//total supply api check if Forbidden
	if ok {
		apiPath := cf.LcdApiPath.SupplyPath
		baseUrl := fmt.Sprintf("%s%s", lcd, apiPath)
		if _, err := utils.HttpGet(baseUrl); err != nil {
			ok = false
		}
	}

	if ok {
		if cf.GrpcRestGateway == lcd && cf.LcdApiPath.ChannelsPath == fmt.Sprintf(apiChannels, version) && cf.LcdApiPath.ClientStatePath == fmt.Sprintf(apiClientState, version) {
			return true
		}

		cf.GrpcRestGateway = lcd
		cf.LcdApiPath.ChannelsPath = fmt.Sprintf(apiChannels, version)
		cf.LcdApiPath.ClientStatePath = fmt.Sprintf(apiClientState, version)
		if err := chainConfigRepo.UpdateLcdApi(cf); err != nil {
			logrus.Errorf("lcd monitor update api error: %v", err)
			return false
		} else {
			return true
		}
	}

	return false
}

// switchLcd If Switch lcd succeeded, return true. Else return false
func switchLcd(chainConf *entity.ChainConfig) bool {
	chainRegistry, err := chainRegistryRepo.FindOne(chainConf.ChainName)
	if err != nil {
		logrus.Errorf("lcd monitor error: %v", err)
		return false
	}

	bz, err := utils.HttpGet(chainRegistry.ChainJsonUrl)
	if err != nil {
		logrus.Errorf("lcd monitor get chain json error: %v", err)
		return false
	}

	var chainRegisterResp vo.ChainRegisterResp
	_ = json.Unmarshal(bz, &chainRegisterResp)
	for _, v := range chainRegisterResp.Apis.Rest {
		if ok := checkAndUpdateLcd(v.Address, chainConf); ok {
			return true
		}
	}

	return false
}

func redisClientStatus(quit chan bool) {
	timeLong := time.Duration(10)
	t := time.NewTimer(timeLong * time.Second)
	for {
		now := time.Now()
		next := now.Add(timeLong)
		t.Reset(next.Sub(now))
		select {
		case <-t.C:
			if cache.RedisStatus() {
				redisStatusMetric.Set(float64(1))
			} else {
				redisStatusMetric.Set(float64(-1))
			}
		case <-quit:
			logrus.Debug("quit signal recv redisClientStatus")
			return
		}
	}
}

func Start(quit chan bool) {
	cronTaskStatusMetric = NewMetricCronWorkStatus()
	redisStatusMetric = NewMetricRedisStatus()
	lcdConnectStatsMetric = NewMetricLcdStatus()

	prometheus.MustRegister(cronTaskStatusMetric)
	prometheus.MustRegister(lcdConnectStatsMetric)
	prometheus.MustRegister(redisStatusMetric)

	go redisClientStatus(quit)
	go lcdConnectionStatus(quit)
}
