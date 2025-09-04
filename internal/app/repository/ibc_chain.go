package repository

import (
	"context"
	"time"

	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/qiniu/qmgo"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	ChainFieldChain            = "chain"
	ChainFieldIbcTokens        = "ibc_tokens"
	ChainFieldRelayers         = "relayers"
	ChainFieldChannels         = "channels"
	ChainFieldConnectedChains  = "connected_chains"
	ChainFieldIbcTokensValue   = "ibc_tokens_value"
	ChainFieldTransferTxs      = "transfer_txs"
	ChainFieldTransferTxsValue = "transfer_txs_value"
	ChainFieldUpdateAt         = "update_at"
)

type IChainRepo interface {
	InserOrUpdate(bulk *qmgo.Bulk, chain entity.IBCChain, update bool) error
	UpdateIbcTokenValue(bulk *qmgo.Bulk, chain string, tokens int64, tokenValue string) error
	UpdateTransferTxs(bulk *qmgo.Bulk, chain string, txs int64, txsValue string) error
	UpdateRelayers(bulk *qmgo.Bulk, chain string, relayers int64) error
	FindAll(skip, limit int64) ([]*entity.IBCChain, error)
	Bulk() *qmgo.Bulk
	Count() (int64, error)
}

var _ IChainRepo = new(IbcChainRepo)

type IbcChainRepo struct {
}

func (repo *IbcChainRepo) coll() *qmgo.Collection {
	return mgo.Database(ibcDatabase).Collection(entity.IBCChain{}.CollectionName())
}

func (repo *IbcChainRepo) Bulk() *qmgo.Bulk {
	return repo.coll().Bulk()
}

func (repo *IbcChainRepo) FindAll(skip, limit int64) ([]*entity.IBCChain, error) {
	var res []*entity.IBCChain
	err := repo.coll().Find(context.Background(), bson.M{}).Skip(skip).Limit(limit).Sort("-" + ChainFieldIbcTokens).All(&res)
	return res, err
}

func (repo *IbcChainRepo) UpdateRelayers(bulk *qmgo.Bulk, chain string, relayers int64) error {
	bulk.UpdateOne(bson.M{ChainFieldChain: chain},
		bson.M{
			"$set": bson.M{
				ChainFieldRelayers: relayers,
			},
		})
	return nil
}

func (repo *IbcChainRepo) InserOrUpdate(bulk *qmgo.Bulk, chain entity.IBCChain, update bool) error {
	if update {
		bulk.UpdateOne(bson.M{ChainFieldChain: chain.Chain},
			bson.M{
				"$set": bson.M{
					ChainFieldChannels:        chain.Channels,
					ChainFieldConnectedChains: chain.ConnectedChains,
					ChainFieldUpdateAt:        time.Now().Unix(),
				},
			})
	} else {
		bulk.InsertOne(chain)
	}

	return nil
}

func (repo *IbcChainRepo) UpdateIbcTokenValue(bulk *qmgo.Bulk, chain string, tokens int64, tokenValue string) error {
	updateData := bson.M{
		ChainFieldIbcTokens:      tokens,
		ChainFieldUpdateAt:       time.Now().Unix(),
		ChainFieldIbcTokensValue: tokenValue,
	}

	bulk.UpdateOne(bson.M{ChainFieldChain: chain},
		bson.M{
			"$set": updateData,
		})
	return nil
}

func (repo *IbcChainRepo) UpdateTransferTxs(bulk *qmgo.Bulk, chain string, txs int64, txsValue string) error {
	bulk.UpdateOne(bson.M{ChainFieldChain: chain},
		bson.M{
			"$set": bson.M{
				ChainFieldTransferTxs:      txs,
				ChainFieldTransferTxsValue: txsValue,
				ChainFieldUpdateAt:         time.Now().Unix(),
			},
		})
	return nil
}

func (repo *IbcChainRepo) Count() (int64, error) {
	return repo.coll().Find(context.Background(), bson.M{}).Count()
}
