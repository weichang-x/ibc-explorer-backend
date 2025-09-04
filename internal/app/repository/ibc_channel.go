package repository

import (
	"context"
	"time"

	"github.com/irisnet/ibc-explorer-backend/internal/app/constant"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/qiniu/qmgo"
	"go.mongodb.org/mongo-driver/bson"
)

type IChannelRepo interface {
	UpdateOneUpdateTime(bulk *qmgo.Bulk, channelId string, updateTime int64) error
	FindAll() (entity.IBCChannelList, error)
	InsertBatch(batch []*entity.IBCChannel) error
	DeleteByChannelIds(channelIds []string) error
	UpdateChannel(bulk *qmgo.Bulk, channel *entity.IBCChannel) error
	List(chainA, chainB string, status entity.ChannelStatus, skip, limit int64) (entity.IBCChannelList, error)
	CountList(chainA, chainB string, status entity.ChannelStatus) (int64, error)
	CountStatus(status entity.ChannelStatus) (int64, error)
	Bulk() *qmgo.Bulk
}

var _ IChannelRepo = new(ChannelRepo)

type ChannelRepo struct {
}

func (repo *ChannelRepo) coll() *qmgo.Collection {
	return mgo.Database(ibcDatabase).Collection(entity.IBCChannel{}.CollectionName())
}
func (repo *ChannelRepo) Bulk() *qmgo.Bulk {
	return repo.coll().Bulk()
}

func (repo *ChannelRepo) UpdateOneUpdateTime(bulk *qmgo.Bulk, channelId string, updateTime int64) error {
	bulk.UpdateOne(bson.M{"channel_id": channelId},
		bson.M{
			"$set": bson.M{
				"channel_update_at": updateTime,
			},
		})
	return nil
}

func (repo *ChannelRepo) analyzeListParam(chainA, chainB string, status entity.ChannelStatus) map[string]interface{} {
	chainCond := make(map[string]interface{}, 0)
	if chainA == constant.AllChain && chainB == constant.AllChain {
		// no condition
	} else if chainA == constant.AllChain {
		chainCond["$or"] = []bson.M{
			{"chain_a": chainB}, {"chain_b": chainB},
		}
	} else if chainB == constant.AllChain {
		chainCond["$or"] = []bson.M{{"chain_a": chainA}, {"chain_b": chainA}}
	} else {
		chainCond["$or"] = []bson.M{
			{"chain_a": chainA, "chain_b": chainB}, {"chain_a": chainB, "chain_b": chainA},
		}
	}

	statusCond := make(map[string]interface{}, 0)
	if status != 0 {
		statusCond["status"] = status
	}

	if len(chainCond) == 0 && len(statusCond) == 0 {
		return bson.M{}
	} else if len(chainCond) == 0 {
		return statusCond
	} else if len(statusCond) == 0 {
		return chainCond
	} else {
		return bson.M{"$and": bson.A{statusCond, chainCond}}
	}
}

func (repo *ChannelRepo) List(chainA, chainB string, status entity.ChannelStatus, skip, limit int64) (entity.IBCChannelList, error) {
	param := repo.analyzeListParam(chainA, chainB, status)
	var res entity.IBCChannelList
	err := repo.coll().Find(context.Background(), param).Limit(limit).Skip(skip).Sort("-transfer_txs").All(&res)
	return res, err
}

func (repo *ChannelRepo) CountList(chainA, chainB string, status entity.ChannelStatus) (int64, error) {
	param := repo.analyzeListParam(chainA, chainB, status)
	count, err := repo.coll().Find(context.Background(), param).Count()
	return count, err
}

func (repo *ChannelRepo) CountStatus(status entity.ChannelStatus) (int64, error) {
	param := bson.M{
		"status": status,
	}
	count, err := repo.coll().Find(context.Background(), param).Count()
	return count, err
}

//func (repo *ChannelRepo) EnsureIndexes() {
//	var indexes []options.IndexModel
//	indexes = append(indexes, options.IndexModel{
//		Key:          []string{"channel_id"},
//		IndexOptions: new(moptions.IndexOptions).SetUnique(true),
//	})
//
//	ensureIndexes(entity.IBCChannel{}.CollectionName(), indexes)
//}

func (repo *ChannelRepo) FindAll() (entity.IBCChannelList, error) {
	var res entity.IBCChannelList
	err := repo.coll().Find(context.Background(), bson.M{}).All(&res)
	return res, err
}

func (repo *ChannelRepo) InsertBatch(batch []*entity.IBCChannel) error {
	if len(batch) == 0 {
		return nil
	}
	now := time.Now().Unix()
	for _, v := range batch {
		v.UpdateAt = now
		v.CreateAt = now
	}
	_, err := repo.coll().InsertMany(context.Background(), batch)
	return err
}

func (repo *ChannelRepo) DeleteByChannelIds(channelIds []string) error {
	if len(channelIds) == 0 {
		return nil
	}

	_, err := repo.coll().RemoveAll(context.Background(), bson.M{"channel_id": bson.M{"$in": channelIds}})
	return err
}

func (repo *ChannelRepo) UpdateChannel(bulk *qmgo.Bulk, channel *entity.IBCChannel) error {
	query := bson.M{
		"channel_id": channel.ChannelId,
	}
	update := bson.M{
		"$set": bson.M{
			"pending_txs":        channel.PendingTxs,
			"status":             channel.Status,
			"operating_period":   channel.OperatingPeriod,
			"latest_open_time":   channel.LatestOpenTime,
			"transfer_txs":       channel.TransferTxs,
			"transfer_txs_value": channel.TransferTxsValue,
			"update_at":          time.Now().Unix(),
		},
	}
	bulk.UpdateOne(query, update)
	return nil
}
