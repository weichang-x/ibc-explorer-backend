package task

import (
	"context"
	"sync"
	"time"

	"github.com/irisnet/ibc-explorer-backend/internal/app/constant"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/qiniu/qmgo"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
)

type IbcSyncAcknowledgeTxTask struct {
}

var _ Task = new(IbcSyncAcknowledgeTxTask)

func (t *IbcSyncAcknowledgeTxTask) Name() string {
	return "ibc_sync_acknowledge_tx_task"
}

func (t *IbcSyncAcknowledgeTxTask) Cron() int {
	if taskConf.CronTimeSyncAckTxTask > 0 {
		return taskConf.CronTimeSyncAckTxTask
	}
	return ThreeMinute
}

func (t *IbcSyncAcknowledgeTxTask) Run() int {
	syncAcknowledge := func(history bool) error {
		startTime := time.Now().Add(-3 * time.Hour).Unix()
		txs, err := ibcTxRepo.GetNeedAcknowledgeTxs(history, startTime)
		if err != nil {
			return err
		}
		if len(txs) == 0 {
			return nil
		}
		var bulk *qmgo.Bulk
		if history {
			bulk = ibcTxRepo.HistoryBulk()
		} else {
			bulk = ibcTxRepo.Bulk()
		}
		for i := range txs {
			err := t.SaveAcknowledgeTx(bulk, txs[i])
			if err != nil && err != qmgo.ErrNoSuchDocuments {
				logrus.Warnf("task %s SaveAcknowledgeTx failed %s, chain_id:%s packet_id:%s",
					t.Name(),
					err.Error(),
					txs[i].ScChain,
					txs[i].ScTxInfo.Msg.CommonMsg().PacketId)
			}
		}
		bulk.SetOrdered(false)
		if _, err := bulk.Run(context.Background()); err != nil {
			logrus.Warnf("task %s bulk SaveAcknowledgeTx bulk  error, %v", t.Name(), err)
		}
		return nil
	}

	syncRecvPacket := func(history bool) error {
		txs, err := ibcTxRepo.GetNeedRecvPacketTxs(history)
		if err != nil {
			return err
		}
		if len(txs) == 0 {
			return nil
		}
		var bulk *qmgo.Bulk
		if history {
			bulk = ibcTxRepo.HistoryBulk()
		} else {
			bulk = ibcTxRepo.Bulk()
		}
		var change bool
		for i := range txs {
			err := SaveRecvPacketTx(bulk, txs[i])
			if err != nil {
				continue
			}
			if !change {
				change = true
			}
		}
		if change {
			bulk.SetOrdered(false)
			if _, err := bulk.Run(context.Background()); err != nil {
				logrus.Warnf("task %s bulk SaveRecvPacketTx bulk  error, %v", t.Name(), err)
			}
		}
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := syncAcknowledge(false)
		logrus.Infof("task %s fix Acknowledge latest end, %v", t.Name(), err)
	}()

	go func() {
		defer wg.Done()
		err := syncRecvPacket(false)
		logrus.Infof("task %s fix RecvPacket latest end, %v", t.Name(), err)
	}()

	wg.Wait()
	return 1
}

func (t *IbcSyncAcknowledgeTxTask) SaveAcknowledgeTx(bulk *qmgo.Bulk, ibcTx *entity.ExIbcTx) error {
	packetId := ibcTx.ScTxInfo.Msg.CommonMsg().PacketId
	ackTx, err := txRepo.GetLatestAcknowledgeTx(ibcTx.ScChain, packetId)
	if err != nil {
		return err
	}
	if ackTx.Height > 0 {
		ibcTx.AckTimeoutTxInfo = &entity.TxInfo{
			Hash:      ackTx.TxHash,
			Height:    ackTx.Height,
			Time:      ackTx.Time,
			Status:    ackTx.Status,
			Fee:       ackTx.Fee,
			Memo:      ackTx.Memo,
			Signers:   ackTx.Signers,
			MsgAmount: nil,
			Msg:       getMsgByType(*ackTx, constant.MsgTypeAcknowledgement, packetId),
		}
		bulk.UpdateId(ibcTx.Id, bson.M{
			"$set": bson.M{
				"ack_timeout_tx_info": ibcTx.AckTimeoutTxInfo,
			},
		})
	}
	return nil
}

func getMsgByType(tx entity.Tx, msgType, packetId string) *model.TxMsg {
	for _, msg := range tx.DocTxMsgs {
		if msg.Type == msgType && msg.CommonMsg().PacketId == packetId {
			return msg
		}
	}
	return nil
}

func SaveRecvPacketTx(bulk *qmgo.Bulk, ibcTx *entity.ExIbcTx) error {
	packetId := ibcTx.ScTxInfo.Msg.CommonMsg().PacketId
	recvTx, err := txRepo.GetLatestRecvPacketTx(ibcTx.DcChain, packetId, ibcTx.DcConnectionId)
	if err != nil {
		if err != qmgo.ErrNoSuchDocuments {
			logrus.Warnf("SaveRecvPacketTx failed %s, chain_id:%s packet_id:%s",
				err.Error(),
				ibcTx.ScChain,
				packetId)
		}
		return err
	}

	if recvTx != nil {
		if ibcTx.DcConnectionId == "" {
			for index, msg := range recvTx.DocTxMsgs {
				if msg.Type == constant.MsgTypeRecvPacket && msg.RecvPacketMsg().PacketId == packetId {
					ibcTx.DcConnectionId = getConnectByRecvPacketEventsNews(recvTx.EventsNew, index)
					break
				}
			}
		}
		ibcTx.DcTxInfo = &entity.TxInfo{
			Hash:      recvTx.TxHash,
			Height:    recvTx.Height,
			Time:      recvTx.Time,
			Status:    recvTx.Status,
			Fee:       recvTx.Fee,
			Memo:      recvTx.Memo,
			Signers:   recvTx.Signers,
			Log:       recvTx.Log,
			MsgAmount: nil,
			Msg:       getMsgByType(*recvTx, constant.MsgTypeRecvPacket, packetId),
		}
		bulk.UpdateId(ibcTx.Id, bson.M{
			"$set": bson.M{
				"dc_tx_info":       ibcTx.DcTxInfo,
				"dc_connection_id": ibcTx.DcConnectionId,
			},
		})
	} else {
		logrus.Debugf("status:%d recv_packet(chain_id:%s)  no found transfer(hash:%s chain_id:%s)",
			ibcTx.Status, ibcTx.DcChain, ibcTx.ScTxInfo.Hash, ibcTx.ScChain)
	}
	return nil
}

func getConnectByRecvPacketEventsNews(eventNews []entity.EventNew, msgIndex int) string {
	var connect string
	for i := range eventNews {
		if eventNews[i].MsgIndex == uint32(msgIndex) {
			for _, val := range eventNews[i].Events {
				if val.Type == "write_acknowledgement" || val.Type == "recv_packet" {
					for _, attribute := range val.Attributes {
						switch attribute.Key {
						case "packet_connection":
							connect = attribute.Value
							//case "packet_ack":
							//	ackData = attribute.Value
						}
					}
				}
			}
		}
	}
	return connect
}
