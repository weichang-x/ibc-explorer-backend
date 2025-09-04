package task

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/irisnet/ibc-explorer-backend/internal/app/repository"
	"github.com/qiniu/qmgo"

	"github.com/irisnet/ibc-explorer-backend/internal/app/constant"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/irisnet/ibc-explorer-backend/internal/app/utils/bech32"
	"github.com/sirupsen/logrus"
)

type segment struct {
	StartTime int64 `json:"start_time"`
	EndTime   int64 `json:"end_time"`
}

func getTxTimeSegment(targetHistory bool, step int64) ([]*segment, error) {
	minTxTime, err := ibcTxRepo.GetMinTxTime(targetHistory)
	if err != nil {
		return nil, err
	}

	return segmentTool(step, minTxTime, time.Now().Unix()), nil
}

func getHistorySegment(step int64) ([]*segment, error) {
	first, err := ibcTxRepo.FirstHistory()
	if err != nil {
		return nil, err
	}

	latest, err := ibcTxRepo.LatestHistory()
	if err != nil {
		return nil, err
	}

	start := time.Unix(first.CreateAt, 0)
	startUnix := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.Local).Unix()
	end := time.Unix(latest.CreateAt, 0)
	endUnix := time.Date(end.Year(), end.Month(), end.Day(), 23, 59, 59, 59, time.Local).Unix()

	var segments []*segment
	for temp := startUnix; temp < endUnix; temp += step {
		segments = append(segments, &segment{
			StartTime: temp,
			EndTime:   temp + step - 1,
		})
	}
	return segments, nil
}

func getSegment(step int64) ([]*segment, error) {
	first, err := ibcTxRepo.First()
	if err != nil {
		return nil, err
	}

	start := time.Unix(first.CreateAt, 0)
	startUnix := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.Local).Unix()
	end := time.Now()
	endUnix := time.Date(end.Year(), end.Month(), end.Day(), 23, 59, 59, 59, time.Local).Unix()

	var segments []*segment
	for temp := startUnix; temp < endUnix; temp += step {
		segments = append(segments, &segment{
			StartTime: temp,
			EndTime:   temp + step - 1,
		})
	}

	return segments, nil
}

func segmentTool(step int64, startTime, endTime int64) []*segment {
	start := time.Unix(startTime, 0)
	startUnix := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.Local).Unix()
	end := time.Unix(endTime, 0)
	endUnix := time.Date(end.Year(), end.Month(), end.Day(), 23, 59, 59, 59, time.Local).Unix()

	var segments []*segment
	for temp := startUnix; temp < endUnix; temp += step {
		segments = append(segments, &segment{
			StartTime: temp,
			EndTime:   temp + step - 1,
		})
	}

	return segments
}

func todayUnix() (int64, int64) {
	now := time.Now()
	startUnix := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local).Unix()
	endUnix := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 59, time.Local).Unix()
	return startUnix, endUnix
}

func yesterdayUnix() (int64, int64) {
	date := time.Now().AddDate(0, 0, -1)
	startUnix := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.Local).Unix()
	endUnix := time.Date(date.Year(), date.Month(), date.Day(), 23, 59, 59, 59, time.Local).Unix()
	return startUnix, endUnix
}

func whetherCheckYesterdayStatistics(taskName string, cronTime int) (bool, *segment) {
	mmdd := time.Now().Format(constant.TimeFormatMMDD)
	incr, err := statisticsCheckRepo.Incr(taskName, mmdd)
	if err != nil {
		logrus.Errorf("task %s statistics incr err, %v", taskName, err)
		return false, nil
	}

	startTime, endTime := yesterdayUnix()
	seg := &segment{
		StartTime: startTime,
		EndTime:   endTime,
	}

	if incr == 1 {
		return true, seg
	}

	var mod int64
	if cronTime <= ThreeMinute {
		mod = 5
	} else if cronTime <= TenMinute {
		mod = 3
	} else if cronTime <= EveryHour {
		mod = 2
	} else {
		mod = 1
	}

	if incr%mod == 0 {
		return true, seg
	} else {
		return false, nil
	}
}

func lastNDaysZeroTimeUnix(n int) (int64, int64) {
	date := time.Now().AddDate(0, 0, -n+1)
	startUnix := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.Local).Unix()
	endUnix := time.Date(date.Year(), date.Month(), date.Day(), 23, 59, 59, 59, time.Local).Unix()
	return startUnix, endUnix
}

func last24hTimeUnix() (int64, int64) {
	now := time.Now()
	year, month, day := now.Date()
	hour := now.Hour()
	min := now.Minute()
	endDate := time.Date(year, month, day, hour, min, 59, 0, time.Local)
	startDate := endDate.AddDate(0, 0, -1)
	return startDate.Unix() + 1, endDate.Unix()
}

func isConnectionErr(err error) bool {
	return true
	//return strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "i/o timeout") ||
	//	strings.Contains(err.Error(), "unsupported protocol scheme")
}

func getAllChainMap() (map[string]*entity.ChainConfig, error) {
	allChainList, err := chainCfgCacheRepo.FindAll()
	if err != nil {
		return nil, err
	}

	allChainMap := make(map[string]*entity.ChainConfig)
	for i := range allChainList {
		allChainMap[allChainList[i].ChainName] = allChainList[i]
	}

	return allChainMap, err
}

func getAllChainInfosMap() (map[string]*entity.ChainConfig, error) {
	allChainList, err := chainConfigRepo.FindAllChainInfos()
	if err != nil {
		return nil, err
	}

	allChainMap := make(map[string]*entity.ChainConfig)
	for _, v := range allChainList {
		allChainMap[v.ChainName] = v
	}

	return allChainMap, err
}

// parseTransferTxEvents parse ibc info from events of transfer tx
func parseTransferTxEvents(msgIndex int, tx *entity.Tx) (dcPort, dcChannel, denomFullPath, sequence, scConnection, addrPrefix string) {
	for i := range tx.EventsNew {
		if int(tx.EventsNew[i].MsgIndex) != msgIndex {
			continue
		}
		for _, evt := range tx.EventsNew[i].Events {
			if evt.Type == "send_packet" {
				for _, attr := range evt.Attributes {
					switch attr.Key {
					case "packet_dst_port":
						dcPort = attr.Value
					case "packet_dst_channel":
						dcChannel = attr.Value
					case "packet_sequence":
						sequence = attr.Value
					case "packet_data":
						var data model.TransferTxPacketData
						_ = json.Unmarshal([]byte(attr.Value), &data)
						denomFullPath = data.Denom
					case "packet_connection":
						scConnection = attr.Value
					default:
					}
				}
			} else if evt.Type == "ibc_transfer" {
				for _, attr := range evt.Attributes {
					if attr.Key == "receiver" {
						addrPrefix, _, _ = bech32.DecodeAndConvert(attr.Value)
					}
				}
			}
		}
	}

	return
}

// parseRecvPacketTxEvents parse ibc info from events of recv packet tx
func parseRecvPacketTxEvents(msgIndex int, tx *entity.Tx) (dcConnection, packetAck string, existPacketAck bool) {
	for i := range tx.EventsNew {
		if int(tx.EventsNew[i].MsgIndex) != msgIndex {
			continue
		}
		for _, evt := range tx.EventsNew[i].Events {
			if evt.Type == "recv_packet" {
				for _, attr := range evt.Attributes {
					switch attr.Key {
					case "packet_connection":
						dcConnection = attr.Value
					default:
					}
				}
			}

			if evt.Type == "write_acknowledgement" {
				for _, attr := range evt.Attributes {
					switch attr.Key {
					case "packet_ack":
						packetAck = attr.Value
						existPacketAck = true
					default:
					}
				}
			}

			if evt.Type == "uptick.erc20.v1.EventIBCERC20" {
				var message string
				var status string
				for _, attr := range evt.Attributes {
					switch attr.Key {
					case "status":
						status = attr.Value
						existPacketAck = true
					case "message":
						message = attr.Value
					default:
					}
				}
				if status == "\"STATUS_SUCCESS\"" {
					packetAck = status
				} else {
					packetAck = fmt.Sprintf("error:\"%s\" ", message)
				}
			}
		}
	}

	return
}

// parseAckPacketTxEvents parse ibc info from events of ack packet tx
func parseAckPacketTxEvents(msgIndex int, tx *entity.Tx) (existTransferEvent bool) {
	for i := range tx.EventsNew {
		if int(tx.EventsNew[i].MsgIndex) != msgIndex {
			continue
		}
		for _, evt := range tx.EventsNew[i].Events {
			if evt.Type == "transfer" {
				existTransferEvent = true
				return
			}
		}
	}
	return
}

func bulkAppendAndRun(collectionName string, appendFn func(bk *qmgo.Bulk)) error {
	bulk := repository.GetDatabase().Collection(collectionName).Bulk()
	appendFn(bulk)
	bulk.SetOrdered(false)
	_, err := bulk.Run(context.Background())
	return err
}
