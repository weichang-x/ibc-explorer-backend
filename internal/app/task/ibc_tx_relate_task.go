package task

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/qiniu/qmgo"

	"github.com/irisnet/ibc-explorer-backend/internal/app/constant"
	"github.com/irisnet/ibc-explorer-backend/internal/app/global"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/dto"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/irisnet/ibc-explorer-backend/internal/app/pkg/ibctool"
	"github.com/irisnet/ibc-explorer-backend/internal/app/utils"
	"github.com/sirupsen/logrus"
)

type IbcTxRelateTask struct {
}

var _ Task = new(IbcTxRelateTask)

//var relateCoordinator *stringQueueCoordinator

func (t *IbcTxRelateTask) Name() string {
	return "ibc_tx_relate_task"
}

func (t *IbcTxRelateTask) Cron() int {
	if taskConf.CronTimeIbcTxRelateTask > 0 {
		return taskConf.CronTimeIbcTxRelateTask
	}
	return ThreeMinute
}

func (t *IbcTxRelateTask) workerNum() int {
	if global.Config.Task.IbcTxRelateWorkerNum > 0 {
		return global.Config.Task.IbcTxRelateWorkerNum
	}
	return ibcTxRelateTaskWorkerNum
}

func (t *IbcTxRelateTask) Run() int {
	chainMap, err := getAllChainMap()
	if err != nil {
		logrus.Errorf("task %s getAllChainMap error, %v", t.Name(), err)
		return -1
	}

	// init coordinator
	chainQueue := new(utils.QueueString)
	for _, v := range chainMap {
		chainQueue.Push(v.ChainName)
	}
	// handle latest ibc tx created on latest one day
	now := time.Now()
	beginTime := now.Add(-24 * time.Hour).Unix()
	workerNum := t.workerNum()
	var waitGroup sync.WaitGroup
	waitGroup.Add(workerNum)
	for i := 1; i <= workerNum; i++ {
		workName := fmt.Sprintf("worker-%d", i)
		go func(wn string) {
			newIbcTxRelateWorker(t.Name(), wn, ibcTxTargetLatest, chainMap).exec(beginTime, chainQueue)
			waitGroup.Done()
		}(workName)
	}
	waitGroup.Wait()
	logrus.Infof("task %s end,time use: %d(s)", t.Name(), time.Now().Unix()-now.Unix())
	return 1
}

func newIbcTxRelateWorker(taskName, workerName, target string, chainMap map[string]*entity.ChainConfig) *ibcTxRelateWorker {
	return &ibcTxRelateWorker{
		taskName:   taskName,
		workerName: workerName,
		target:     target,
		chainMap:   chainMap,
	}
}

type ibcTxRelateWorker struct {
	taskName   string
	workerName string
	target     string
	chainMap   map[string]*entity.ChainConfig
}

func (w *ibcTxRelateWorker) exec(beginTime int64, queue *utils.QueueString) {
	logrus.Infof("task %s worker %s start", w.taskName, w.workerName)
	endTime := time.Now().Unix()
	segments := segmentTool(segmentStepLatest, beginTime, endTime)
	for {
		chain, err := queue.Pop()
		if err != nil {
			logrus.Infof("task %s worker %s exit", w.taskName, w.workerName)
			break
		}

		if cf, ok := w.chainMap[chain]; ok && cf.Status == entity.ChainStatusClosed {
			logrus.Infof("task %s worker %s chain %s is closed", w.taskName, w.workerName, chain)
			continue
		}

		logrus.Infof("task %s worker %s get chain: %v", w.taskName, w.workerName, chain)
		for _, seg := range segments {
			startTime := time.Now().Unix()
			if endTime < seg.EndTime {
				seg.EndTime = endTime
			}
			if err = w.relateTx(chain, seg.StartTime, seg.EndTime); err != nil {
				logrus.Errorf("task %s worker %s relate chain %s  tx error, %v", w.taskName, w.workerName, chain, err)
			}
			logrus.Infof("task %s worker %s relate chain %s tx end,time:(%d %d) use: %d(s)", w.taskName, w.workerName, chain, seg.StartTime, seg.EndTime, time.Now().Unix()-startTime)
		}
	}
}

//func (w *ibcTxRelateWorker) getChain() (string, error) {
//	if w.target == ibcTxTargetHistory {
//		return relateHistoryCoordinator.getOne()
//	}
//	return relateCoordinator.getOne()
//}

func (w *ibcTxRelateWorker) relateTx(chain string, beginTime, endTime int64) error {

	denomMap, err := w.getChainDenomMap(chain)
	if err != nil {
		return err
	}
	curTime := beginTime
	for {
		txList, err := w.getToBeRelatedTxs(chain, curTime, endTime, constant.DefaultLimit)
		if err != nil {
			logrus.Errorf("task %s worker %s chain %s getToBeRelatedTxs error, %v", w.taskName, w.workerName, chain, err)
			return err
		}

		if len(txList) == 0 {
			return nil
		} else if len(txList) == constant.DefaultLimit {
			txsAtTxTime, err := ibcTxRepo.FindProcessingTxsAtTxTime(chain, txList[len(txList)-1].TxTime, w.target == ibcTxTargetHistory)
			if err != nil {
				logrus.Errorf("task %s worker %s chain %s FindProcessingTxsAtTxTime error, %v", w.taskName, w.workerName, chain, err)
				return err
			}
			txList = append(txList, txsAtTxTime...)
			txList = distinctSliceIbcTxs(txList)
		}
		w.handlerIbcTxs(chain, txList, denomMap)
		if len(txList) < constant.DefaultLimit {
			break
		}
		curTime = txList[len(txList)-1].TxTime
		logrus.Infof("task %s worker %s relate chain %s  handlerIbcTxs start:%d end:%d current:%d", w.taskName, w.workerName, chain, beginTime, endTime, curTime)
	}

	return nil
}
func distinctSliceIbcTxs(ibcTxs []*entity.ExIbcTx) []*entity.ExIbcTx {
	set := make(map[string]int, len(ibcTxs))
	res := make([]*entity.ExIbcTx, 0, len(ibcTxs))
	for i := range ibcTxs {
		if _, ok := set[ibcTxs[i].ScTxInfo.Hash]; ok {
			continue
		}
		res = append(res, ibcTxs[i])
		set[ibcTxs[i].ScTxInfo.Hash] = 0
	}
	return res
}

func (w *ibcTxRelateWorker) handlerIbcTxs(scChain string, ibcTxs []*entity.ExIbcTx, denomMap map[string]*entity.IBCDenom) {
	recvPacketTxMap, ackTxMap, timeoutTxMap, timeoutIbcTxMap, noFoundAckMap := w.packetIdTx(scChain, ibcTxs)

	var (
		ibcDenomNewList entity.IBCDenomList
		bulk            *qmgo.Bulk
	)
	if w.target == ibcTxTargetHistory {
		bulk = ibcTxRepo.HistoryBulk()
	} else {
		bulk = ibcTxRepo.Bulk()
	}

	for i := range ibcTxs {
		if ibcTxs[i].DcChain == "" || ibcTxs[i].ScTxInfo == nil || ibcTxs[i].ScTxInfo.Msg == nil {
			w.setNextTryTime(ibcTxs[i])
		} else {
			packetId := ibcTxs[i].ScTxInfo.Msg.CommonMsg().PacketId
			if recvTxs, ok := recvPacketTxMap[w.genPacketTxMapKey(ibcTxs[i].DcChain, packetId)]; ok {
				ackTxs := ackTxMap[w.genPacketTxMapKey(ibcTxs[i].ScChain, packetId)]
				ibcDenom := w.loadRecvPacketTx(ibcTxs[i], recvTxs, ackTxs)
				if ibcDenom != nil && denomMap[ibcDenom.Denom] == nil {
					denomMap[ibcDenom.Denom] = ibcDenom
					ibcDenomNewList = append(ibcDenomNewList, ibcDenom)
				}
			}

			if timeoutTx, ok := timeoutTxMap[w.genPacketTxMapKey(ibcTxs[i].ScChain, packetId)]; ok && ibcTxs[i].Status == entity.IbcTxStatusProcessing {
				recvTxs := recvPacketTxMap[w.genPacketTxMapKey(ibcTxs[i].DcChain, packetId)]
				w.loadTimeoutPacketTx(ibcTxs[i], timeoutTx, recvTxs)
			}
		}

		if ibcTxs[i].Status == entity.IbcTxStatusProcessing {
			w.setNextTryTime(ibcTxs[i])
			ibcTxs[i] = w.updateProcessInfo(ibcTxs[i], timeoutIbcTxMap, noFoundAckMap)
		}
		var repaired bool
		ibcTxs[i], repaired = w.repairTxInfo(ibcTxs[i])
		if err := w.updateIbcTx(bulk, ibcTxs[i], repaired); err != nil {
			logrus.Errorf("task %s worker %s chain %s updateIbcTx error, _id: %s, %v", w.taskName, w.workerName, scChain, ibcTxs[i].Id, err)
		}
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		bulk.SetOrdered(false)
		if _, err := bulk.Run(context.Background()); err != nil {
			logrus.Errorf("task %s worker %s chain %s bulk updateIbcTx error, %v", w.taskName, w.workerName, scChain, err)
		}
	}()
	go func() {
		defer wg.Done()
		// add denom
		if len(ibcDenomNewList) > 0 {
			if err := denomRepo.InsertBatch(ibcDenomNewList); err != nil {
				logrus.Errorf("task %s worker %s chain %s insert denoms error, %v", w.taskName, w.workerName, scChain, err)
			}
		}
	}()
	wg.Wait()
}

func (w *ibcTxRelateWorker) updateProcessInfo(ibcTx *entity.ExIbcTx, timeOutMap map[string]struct{}, noFoundAckMap map[string]struct{}) *entity.ExIbcTx {
	if ibcTx.Status == entity.IbcTxStatusProcessing {
		if ibcTx.DcChain == "" {
			ibcTx.ProcessInfo = constant.NoFoundDcChain
		} else {
			if _, ok := timeOutMap[ibcTx.Id.Hex()]; ok {
				ibcTx.ProcessInfo = constant.NoFoundSuccessTimeoutPacket
			} else if _, ok := noFoundAckMap[ibcTx.Id.Hex()]; ok {
				ibcTx.ProcessInfo = constant.NoFoundSuccessAcknowledgePacket
			} else {
				ibcTx.ProcessInfo = constant.NoFoundSuccessRecvPacket
			}
		}
	} else {
		ibcTx.ProcessInfo = ""
	}
	return ibcTx
}

func (w *ibcTxRelateWorker) loadRecvPacketTx(ibcTx *entity.ExIbcTx, recvTxs, ackTxs []*entity.Tx) *entity.IBCDenom {
	refundedMatchAckTx := func() *entity.Tx {
		var matchTx *entity.Tx
		for i := range ackTxs {
			for msgIndex, msg := range ackTxs[i].DocTxMsgs {
				if msg.Type != constant.MsgTypeAcknowledgement || msg.CommonMsg().PacketId != ibcTx.ScTxInfo.Msg.CommonMsg().PacketId {
					continue
				}
				existTransferEvent := parseAckPacketTxEvents(msgIndex, ackTxs[i])
				if existTransferEvent {
					matchTx = ackTxs[i]
				}
			}
		}
		return matchTx
	}

	successMatchAckTx := func() *entity.Tx {
		var matchTx *entity.Tx
		for i := range ackTxs {
			for _, msg := range ackTxs[i].DocTxMsgs {
				if msg.Type != constant.MsgTypeAcknowledgement || msg.CommonMsg().PacketId != ibcTx.ScTxInfo.Msg.CommonMsg().PacketId {
					continue
				}
				if matchTx == nil {
					matchTx = ackTxs[i]
				} else {
					if ackTxs[i].Time > matchTx.Time {
						matchTx = ackTxs[i]
					}
				}
			}
		}
		return matchTx
	}

	var ibcDenom *entity.IBCDenom
	var matchAckTx *entity.Tx
	for i := range recvTxs {
		if recvTxs[i].Status == entity.TxStatusFailed {
			continue
		}
		for msgIndex, msg := range recvTxs[i].DocTxMsgs {
			if msg.Type != constant.MsgTypeRecvPacket || msg.CommonMsg().PacketId != ibcTx.ScTxInfo.Msg.CommonMsg().PacketId {
				continue
			}
			dcConnection, packetAck, existPacketAck := parseRecvPacketTxEvents(msgIndex, recvTxs[i])
			if existPacketAck {
				if strings.Contains(packetAck, "error") {
					matchAckTx = refundedMatchAckTx()
					if matchAckTx == nil {
						return nil
					}
					ibcTx.Status = entity.IbcTxStatusRefunded
				} else {
					matchAckTx = successMatchAckTx()
					ibcTx.Status = entity.IbcTxStatusSuccess
				}
			} else {
				//recv_packet tx events not exist event type write_acknowledgement
				for index := range ackTxs {
					for _, v := range ackTxs[index].DocTxMsgs {
						if v.Type != constant.MsgTypeAcknowledgement || v.CommonMsg().PacketId != msg.CommonMsg().PacketId {
							continue
						}
						matchAckTx = ackTxs[index]
						if strings.Contains(v.AckPacketMsg().Acknowledgement, "error") {
							ibcTx.Status = entity.IbcTxStatusRefunded
						} else {
							ibcTx.Status = entity.IbcTxStatusSuccess
						}
					}
				}
			}

			ibcTx.DcConnectionId = dcConnection
			ibcTx.DcTxInfo = &entity.TxInfo{
				Hash:      recvTxs[i].TxHash,
				Status:    recvTxs[i].Status,
				Time:      recvTxs[i].Time,
				Height:    recvTxs[i].Height,
				Fee:       recvTxs[i].Fee,
				MsgAmount: nil,
				Msg:       msg,
				Memo:      recvTxs[i].Memo,
				Signers:   recvTxs[i].Signers,
				Log:       recvTxs[i].Log,
			}
			ibcTx.UpdateAt = time.Now().Unix()

			dcDenomFullPath, isCrossBack := ibctool.CalculateNextDenomPath(msg.RecvPacketMsg().Packet)
			dcDenom := ibctool.CalculateIBCHash(dcDenomFullPath)
			ibcTx.Denoms.DcDenom = dcDenom // set ibc tx dc denom
			if ibcTx.Status == entity.IbcTxStatusSuccess {
				if !isCrossBack {
					dcDenomPath, rootDenom := ibctool.SplitFullPath(dcDenomFullPath)
					ibcDenom = &entity.IBCDenom{
						Symbol:         "",
						Chain:          ibcTx.DcChain,
						Denom:          dcDenom,
						PrevDenom:      ibcTx.Denoms.ScDenom,
						PrevChain:      ibcTx.ScChain,
						BaseDenom:      ibcTx.BaseDenom,
						BaseDenomChain: ibcTx.BaseDenomChain,
						DenomPath:      dcDenomPath,
						IBCHops:        ibctool.IBCHops(dcDenomPath),
						IsBaseDenom:    false,
						RootDenom:      rootDenom,
						CreateAt:       time.Now().Unix(),
						UpdateAt:       time.Now().Unix(),
					}
				}
			}
		}
	}

	if matchAckTx != nil && ibcTx.Status != entity.IbcTxStatusProcessing {
		for _, msg := range matchAckTx.DocTxMsgs {
			if msg.Type != constant.MsgTypeAcknowledgement || msg.CommonMsg().PacketId != ibcTx.ScTxInfo.Msg.CommonMsg().PacketId {
				continue
			}
			ibcTx.AckTimeoutTxInfo = &entity.TxInfo{
				Hash:      matchAckTx.TxHash,
				Status:    matchAckTx.Status,
				Time:      matchAckTx.Time,
				Height:    matchAckTx.Height,
				Fee:       matchAckTx.Fee,
				MsgAmount: nil,
				Msg:       msg,
				Memo:      matchAckTx.Memo,
				Signers:   matchAckTx.Signers,
				Log:       matchAckTx.Log,
			}
		}
	}
	return ibcDenom
}

func (w *ibcTxRelateWorker) loadAckPacketTx(ibcTx *entity.ExIbcTx, tx *entity.Tx) {
	for _, msg := range tx.DocTxMsgs {
		if msg.Type == constant.MsgTypeAcknowledgement && msg.CommonMsg().PacketId == ibcTx.ScTxInfo.Msg.CommonMsg().PacketId {
			if strings.Contains(msg.AckPacketMsg().Acknowledgement, "error") { // ack error
				ibcTx.Status = entity.IbcTxStatusRefunded
				ibcTx.AckTimeoutTxInfo = &entity.TxInfo{
					Hash:      tx.TxHash,
					Status:    tx.Status,
					Time:      tx.Time,
					Height:    tx.Height,
					Fee:       tx.Fee,
					MsgAmount: nil,
					Msg:       msg,
					Memo:      tx.Memo,
					Signers:   tx.Signers,
					Log:       tx.Log,
				}
				ibcTx.UpdateAt = time.Now().Unix()
			} else {
				w.setNextTryTime(ibcTx)
			}
			return
		}
	}
}

func (w *ibcTxRelateWorker) loadTimeoutPacketTx(ibcTx *entity.ExIbcTx, tx *entity.Tx, recvTxs []*entity.Tx) {
	packetId := ibcTx.ScTxInfo.Msg.CommonMsg().PacketId
	for _, msg := range tx.DocTxMsgs {
		if msg.Type == constant.MsgTypeTimeoutPacket && msg.CommonMsg().PacketId == packetId {
			ibcTx.Status = entity.IbcTxStatusRefunded
			ibcTx.AckTimeoutTxInfo = &entity.TxInfo{
				Hash:      tx.TxHash,
				Status:    tx.Status,
				Time:      tx.Time,
				Height:    tx.Height,
				Fee:       tx.Fee,
				MsgAmount: nil,
				Msg:       msg,
				Memo:      tx.Memo,
				Signers:   tx.Signers,
				Log:       tx.Log,
			}
		}
	}

	if ibcTx.Status != entity.IbcTxStatusProcessing {
		var matchRecvTx *entity.Tx
		var matchRecvTxMsg *model.TxMsg
		for i := range recvTxs {
			for _, msg := range recvTxs[i].DocTxMsgs {
				if msg.Type != constant.MsgTypeRecvPacket || msg.CommonMsg().PacketId != ibcTx.ScTxInfo.Msg.CommonMsg().PacketId {
					continue
				}

				if matchRecvTx == nil {
					matchRecvTx = recvTxs[i]
					matchRecvTxMsg = msg
				} else {
					if recvTxs[i].Time > matchRecvTx.Time {
						matchRecvTx = recvTxs[i]
						matchRecvTxMsg = msg
					}
				}
			}
		}

		if matchRecvTx != nil {
			ibcTx.DcTxInfo = &entity.TxInfo{
				Hash:      matchRecvTx.TxHash,
				Status:    matchRecvTx.Status,
				Time:      matchRecvTx.Time,
				Height:    matchRecvTx.Height,
				Fee:       matchRecvTx.Fee,
				MsgAmount: nil,
				Msg:       matchRecvTxMsg,
				Memo:      matchRecvTx.Memo,
				Signers:   matchRecvTx.Signers,
				Log:       matchRecvTx.Log,
			}
		}
	}
}

func (w *ibcTxRelateWorker) setNextTryTime(ibcTx *entity.ExIbcTx) {
	now := time.Now().Unix()
	ibcTx.RetryTimes += 1
	ibcTx.NextTryTime = now + (ibcTx.RetryTimes * 2)
	ibcTx.UpdateAt = time.Now().Unix()
}

func (w *ibcTxRelateWorker) repairTxInfo(ibcTx *entity.ExIbcTx) (*entity.ExIbcTx, bool) {
	var repaired bool
	if ibcTx.DcClientId == "" {
		if cf, ok := w.chainMap[ibcTx.DcChain]; ok {
			ibcTx.DcClientId = cf.GetChannelClient(ibcTx.DcPort, ibcTx.DcChannel)
			repaired = true
		}
	}

	if ibcTx.ScClientId == "" {
		if cf, ok := w.chainMap[ibcTx.ScChain]; ok {
			ibcTx.ScClientId = cf.GetChannelClient(ibcTx.ScPort, ibcTx.ScChannel)
			repaired = true
		}
	}

	if ibcTx.ScConnectionId == "" {
		packetId := ibcTx.ScTxInfo.Msg.CommonMsg().PacketId
		status := entity.TxStatusSuccess
		if transferTxs, err := txRepo.FindByPacketIds(ibcTx.ScChain, constant.MsgTypeTransfer, []string{packetId}, &status); err == nil && len(transferTxs) > 0 {
			for msgIndex, msg := range transferTxs[0].DocTxMsgs {
				if msg.Type == constant.MsgTypeTransfer && msg.CommonMsg().PacketId == packetId {
					_, _, _, _, ibcTx.ScConnectionId, _ = parseTransferTxEvents(msgIndex, transferTxs[0])
					repaired = true
					break
				}
			}
		}
	}
	return ibcTx, repaired
}

func (w *ibcTxRelateWorker) genPacketTxMapKey(chain, packetId string) string {
	return fmt.Sprintf("%s_%s", chain, packetId)
}

func (w *ibcTxRelateWorker) packetIdTx(scChain string, ibcTxList []*entity.ExIbcTx) (recvPacketTxMap, ackTxMap map[string][]*entity.Tx, timeoutTxMap map[string]*entity.Tx, timeoutIbcTxMap, noFoundAckMap map[string]struct{}) {
	packetIdsMap := w.packetIdsMap(ibcTxList)
	chainLatestBlockMap := w.findLatestBlock(scChain, ibcTxList)
	var timeoutTxPacketIds, ackPacketIds []string
	recvPacketTxMap = make(map[string][]*entity.Tx)
	ackTxMap = make(map[string][]*entity.Tx)
	timeoutTxMap = make(map[string]*entity.Tx)
	timeoutIbcTxMap = make(map[string]struct{})
	noFoundAckMap = make(map[string]struct{})
	packetIdRecordMap := make(map[string]string, len(packetIdsMap))
	status := entity.TxStatusSuccess

	for dcChain, packetIds := range packetIdsMap {
		latestBlock := chainLatestBlockMap[dcChain]
		var recvPacketIds []string
		for _, packet := range packetIds { // recv && refunded
			packetIdRecordMap[dcChain+packet.PacketId] = packet.ObjectId
			recvPacketIds = append(recvPacketIds, packet.PacketId)
			timeoutStr := strconv.FormatInt(packet.TimeOutTime, 10)
			if len(timeoutStr) > 10 {
				if len(timeoutStr) == 19 && time.Now().UnixNano() > packet.TimeOutTime { // Nano
					timeoutTxPacketIds = append(timeoutTxPacketIds, packet.PacketId)
					timeoutIbcTxMap[packet.ObjectId] = struct{}{}
				} else {
					//logrus.Warningf("unkonwn timeout time %s, chain: %s, packet id: %s", timeoutStr, dcChain, packet.PacketId)
					timeoutTxPacketIds = append(timeoutTxPacketIds, packet.PacketId)
				}
			} else if latestBlock != nil {
				if latestBlock.Height > packet.TimeoutHeight || latestBlock.Time > packet.TimeOutTime {
					timeoutTxPacketIds = append(timeoutTxPacketIds, packet.PacketId)
					timeoutIbcTxMap[packet.ObjectId] = struct{}{}
				}
			}
		}

		recvTxList, err := txRepo.FindByPacketIds(dcChain, constant.MsgTypeRecvPacket, recvPacketIds, nil)
		if err != nil {
			logrus.Errorf("task %s worker %s dc chain %s find recv txs error, %v", w.taskName, w.workerName, dcChain, err)
			continue
		}

		for _, tx := range recvTxList {
			for _, msg := range tx.DocTxMsgs {
				if msg.Type == constant.MsgTypeRecvPacket {
					if recvMsg := msg.RecvPacketMsg(); recvMsg.PacketId != "" {
						mk := w.genPacketTxMapKey(dcChain, recvMsg.PacketId)
						recvPacketTxMap[mk] = append(recvPacketTxMap[mk], tx)
						if tx.Status == entity.TxStatusSuccess {
							ackPacketIds = append(ackPacketIds, recvMsg.PacketId)
							if oid, ok := packetIdRecordMap[dcChain+recvMsg.PacketId]; ok {
								noFoundAckMap[oid] = struct{}{}
							}
						}
					} else {
						logrus.Errorf("%s recv packet tx(%s) packet id is empty", dcChain, tx.TxHash)
					}
				}
			}
		}
	}

	if len(timeoutTxPacketIds) > 0 {
		timeoutTxList, err := txRepo.FindByPacketIds(scChain, constant.MsgTypeTimeoutPacket, timeoutTxPacketIds, &status)
		if err == nil {
			for _, tx := range timeoutTxList {
				for _, msg := range tx.DocTxMsgs {
					if msg.Type == constant.MsgTypeTimeoutPacket {
						if timeoutMsg := msg.TimeoutPacketMsg(); timeoutMsg.PacketId != "" {
							timeoutTxMap[w.genPacketTxMapKey(scChain, timeoutMsg.PacketId)] = tx
						} else {
							logrus.Errorf("%s timeout packet tx(%s) packet id is empty", scChain, tx.TxHash)
						}
					}
				}
			}
		} else {
			logrus.Errorf("task %s worker %s sc chain %s find timeout txs error, %v", w.taskName, w.workerName, scChain, err)
		}
	}

	if len(ackPacketIds) > 0 {
		ackTxList, err := txRepo.FindByPacketIds(scChain, constant.MsgTypeAcknowledgement, ackPacketIds, &status)
		if err == nil {
			for _, tx := range ackTxList {
				for _, msg := range tx.DocTxMsgs {
					if msg.Type == constant.MsgTypeAcknowledgement {
						if ackMsg := msg.AckPacketMsg(); ackMsg.PacketId != "" {
							mk := w.genPacketTxMapKey(scChain, ackMsg.PacketId)
							ackTxMap[mk] = append(ackTxMap[mk], tx)
						} else {
							logrus.Errorf("%s ack packet tx(%s) packet id is empty", scChain, tx.TxHash)
						}
					}
				}
			}
		} else {
			logrus.Errorf("task %s worker %s sc chain %s find ack txs error, %v", w.taskName, w.workerName, scChain, err)
		}
	}
	return
}

func (w *ibcTxRelateWorker) packetIdsMap(ibcTxList []*entity.ExIbcTx) map[string][]*dto.PacketIdDTO {
	res := make(map[string][]*dto.PacketIdDTO)
	for _, tx := range ibcTxList {
		if tx.DcChain == "" || tx.ScTxInfo == nil || tx.ScTxInfo.Msg == nil {
			logrus.Warningf("ibc tx dc_chain_id or sc_tx_info exception, record_id: %s", tx.Id)
			continue
		}

		transferMsg := tx.ScTxInfo.Msg.TransferMsg()
		if transferMsg.PacketId == "" {
			logrus.Warningf("ibc tx packet_id is empty, hash: %s", tx.ScTxInfo.Hash)
			continue
		}

		res[tx.DcChain] = append(res[tx.DcChain], &dto.PacketIdDTO{
			DcChain:       tx.DcChain,
			TimeoutHeight: transferMsg.TimeoutHeight.RevisionHeight,
			PacketId:      transferMsg.PacketId,
			TimeOutTime:   transferMsg.TimeoutTimestamp,
			ObjectId:      tx.Id.Hex(),
		})
	}
	return res
}

func (w *ibcTxRelateWorker) findLatestBlock(scChain string, ibcTxList []*entity.ExIbcTx) map[string]*dto.HeightTimeDTO {
	blockMap := make(map[string]*dto.HeightTimeDTO)

	findFunc := func(chain string) {
		block, err := syncBlockRepo.FindLatestBlock(chain)
		if err != nil {
			logrus.Errorf("task %s worker %s chain %s findLatestBlock error, %v", w.taskName, w.workerName, chain, err)
		} else {
			blockMap[chain] = &dto.HeightTimeDTO{
				Height: block.Height,
				Time:   block.Time,
			}
		}
	}

	findFunc(scChain)
	for _, tx := range ibcTxList {
		if tx.DcChain == "" {
			continue
		}

		if _, ok := blockMap[tx.DcChain]; !ok {
			findFunc(tx.DcChain)
		}
	}

	return blockMap
}

func (w *ibcTxRelateWorker) getToBeRelatedTxs(chain string, startTime, endTime, limit int64) ([]*entity.ExIbcTx, error) {
	if w.target == ibcTxTargetHistory {
		return ibcTxRepo.FindProcessingHistoryTxs(chain, startTime, endTime, limit)
	}
	return ibcTxRepo.FindProcessingTxs(chain, startTime, endTime, limit)
}

func (w *ibcTxRelateWorker) updateIbcTx(bulk *qmgo.Bulk, ibcTx *entity.ExIbcTx, repaired bool) error {
	if w.target == ibcTxTargetHistory {
		return ibcTxRepo.UpdateIbcHistoryTx(bulk, ibcTx, repaired)
	}
	return ibcTxRepo.UpdateIbcTx(bulk, ibcTx, repaired)
}

func (w *ibcTxRelateWorker) getChainDenomMap(chain string) (map[string]*entity.IBCDenom, error) {
	denomList, err := denomRepo.FindByChain(chain)
	if err != nil {
		logrus.Errorf("task %s worker %s getChainDenomMap %s error, %v", w.taskName, w.workerName, chain, err)
		return nil, err
	}

	denomMap := make(map[string]*entity.IBCDenom, len(denomList))
	for _, v := range denomList {
		denomMap[v.Denom] = v
	}
	return denomMap, nil
}
