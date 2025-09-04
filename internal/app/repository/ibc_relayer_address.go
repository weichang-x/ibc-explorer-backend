package repository

import (
	"context"

	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/qiniu/qmgo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type IRelayerAddressRepo interface {
	InsertOne(addr *entity.IBCRelayerAddress) error
	InsertMany(batch []*entity.IBCRelayerAddress) error
	FindNoPubKey(startTime int64) ([]*entity.IBCRelayerAddress, error)
	FindByPubKey(pubKey string) ([]*entity.IBCRelayerAddress, error)
	FindToBeGathered(startTime int64) ([]*entity.IBCRelayerAddress, error)
	UpdatePubKey(bulk *qmgo.Bulk, address, chain, pubKey string) error
	UpdateGatherStatus(bulk *qmgo.Bulk, address, chain string, status entity.GatherStatus) error
	Bulk() *qmgo.Bulk
}

var _ IRelayerAddressRepo = new(RelayerAddressRepo)

type RelayerAddressRepo struct {
}

func (repo *RelayerAddressRepo) coll() *qmgo.Collection {
	return mgo.Database(ibcDatabase).Collection(entity.IBCRelayerAddressCollName)
}

func (repo *RelayerAddressRepo) Bulk() *qmgo.Bulk {
	return repo.coll().Bulk()
}

func (repo *RelayerAddressRepo) InsertOne(addr *entity.IBCRelayerAddress) error {
	_, err := repo.coll().InsertOne(context.Background(), addr)
	return err
}

func (repo *RelayerAddressRepo) InsertMany(batch []*entity.IBCRelayerAddress) error {
	_, err := repo.coll().InsertMany(context.Background(), batch, insertIgnoreErrOpt)
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

func (repo *RelayerAddressRepo) FindNoPubKey(startTime int64) ([]*entity.IBCRelayerAddress, error) {
	query := bson.M{
		"pub_key": "",
	}
	if startTime > 0 {
		query["create_at"] = bson.M{
			"$gte": startTime,
		}
	}

	var res []*entity.IBCRelayerAddress
	err := repo.coll().Find(context.Background(), query).All(&res)
	return res, err
}

func (repo *RelayerAddressRepo) FindByPubKey(pubKey string) ([]*entity.IBCRelayerAddress, error) {
	query := bson.M{
		"pub_key": pubKey,
	}

	var res []*entity.IBCRelayerAddress
	err := repo.coll().Find(context.Background(), query).All(&res)
	return res, err
}

func (repo *RelayerAddressRepo) FindToBeGathered(startTime int64) ([]*entity.IBCRelayerAddress, error) {
	query := bson.M{
		"gather_status": entity.GatherStatusTODO,
		"pub_key": bson.M{
			"$ne": "",
		},
	}
	if startTime > 0 {
		query["create_at"] = bson.M{
			"$gte": startTime,
		}
	}

	var res []*entity.IBCRelayerAddress
	err := repo.coll().Find(context.Background(), query).All(&res)
	return res, err
}

func (repo *RelayerAddressRepo) UpdatePubKey(bulk *qmgo.Bulk, address, chain, pubKey string) error {
	bulk.UpdateOne(bson.M{
		"address": address,
		"chain":   chain,
	}, bson.M{
		"$set": bson.M{
			"pub_key": pubKey,
		},
	})
	return nil
}

func (repo *RelayerAddressRepo) UpdateGatherStatus(bulk *qmgo.Bulk, address, chain string, status entity.GatherStatus) error {
	bulk.UpdateOne(bson.M{
		"address": address,
		"chain":   chain,
	}, bson.M{
		"$set": bson.M{
			"gather_status": status,
		},
	})
	return nil
}
