package task

import (
	"fmt"
	"testing"

	"github.com/irisnet/ibc-explorer-backend/internal/app/utils"
)

func Test_IbxTxRelateTask(t *testing.T) {
	new(IbcTxRelateTask).Run()
}

func Test_IbxTxRelateHistoryTask(t *testing.T) {
	new(IbcTxRelateHistoryTask).Run()
}

func Test_HandlerSourceTx(t *testing.T) {
	chainMap, _ := getAllChainMap()
	w := newSyncTransferTxWorker("transfer", "worker", chainMap)
	chain := "irishub_qa"
	denomMap, _ := w.getChainDenomMap(chain)
	hashes := []string{"6BDD5E93A3E9DEC5402D8674508A15C52FC80105089DADA896B1AC67F65D275C"}
	txList, _ := txRepo.GetTxByHashes(chain, hashes)
	ibcTxList, _ := w.handleSourceTx(chain, txList, denomMap)

	rw := newIbcTxRelateWorker("relate", "worker", ibcTxTargetLatest, chainMap)
	rw.handlerIbcTxs(chain, ibcTxList, denomMap)
	t.Log(utils.MustMarshalJsonToStr(ibcTxList))
}

func Test_HandlerIbcTxs(t *testing.T) {
	chainMap, _ := getAllChainMap()
	w := newSyncTransferTxWorker("transfer", "worker", chainMap)
	chain := "osmosis"
	denomMap, _ := w.getChainDenomMap(chain)
	ibcTx, _ := ibcTxRepo.TxDetail("F289DE49E4CAFF7EBB41794ED6452D2C7F6A3740C1E5FF08D84002665D474D26", true)
	ibcTx[0].ScTxInfo.Msg.Msg["receiver"] = ""
	ibcTx[0].DcAddr = ""
	rw := newIbcTxRelateWorker("relate", "worker", ibcTxTargetLatest, chainMap)
	rw.handlerIbcTxs(chain, ibcTx, denomMap)
	//ibcTx[0].DcTxInfo.Msg.Msg["receiver"] = ""
	//ibcTx[0].AckTimeoutTxInfo.Msg.Msg["receiver"] = ""
	t.Log(utils.MustMarshalJsonToStr(ibcTx))
}

func Test_parseRecvPacketTxEvents(t *testing.T) {
	txs, err := txRepo.GetTxByHashes("uptick", []string{"C476E603D7A3329FCB8486897B465815F10E3A6B70F7BC89657986409CBC3FB6"})
	if err != nil {
		t.Log(err.Error())
	}

	dcConnection, packetAck, exists := parseRecvPacketTxEvents(1, txs[0])
	fmt.Println(dcConnection)
	fmt.Println(packetAck)
	fmt.Println(exists)
}

func TestLoadTaskConf(t *testing.T) {
	txs, _ := ibcTxRepo.FindProcessingHistoryTxs("umee", 1718574360, 1718582399, 500)
	fmt.Println(txs[len(txs)-1].TxTime)
	tx1, _ := ibcTxRepo.FindProcessingTxsAtTxTime("umee", txs[len(txs)-1].TxTime, true)
	fmt.Println(len(txs))
	fmt.Println(len(tx1))
	txs = append(txs, tx1...)
	fmt.Println(len(txs))
	txs = distinctSliceIbcTxs(txs)
	fmt.Println(len(txs))
}
