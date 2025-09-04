package repository

import (
	"context"
	"time"

	"github.com/irisnet/ibc-explorer-backend/internal/app/constant"
	"github.com/qiniu/qmgo/operator"

	"github.com/irisnet/ibc-explorer-backend/internal/app/model/dto"
	"github.com/irisnet/ibc-explorer-backend/internal/app/model/entity"
	"github.com/qiniu/qmgo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type IDenomRepo interface {
	FindAll(createAt int64, limit int64) (entity.IBCDenomList, error)
	FindBaseDenom() (entity.IBCDenomList, error)
	FindByBaseDenom(baseDenom, baseDenomChain string) (entity.IBCDenomList, error)
	FindByChain(chain string) (entity.IBCDenomList, error)
	FindByDenom(denom string) (entity.IBCDenomList, error)
	FindByDenomChain(denom, chain string) (*entity.IBCDenom, error)
	GetDenomGroupByChain() ([]*dto.GetDenomGroupByChainDTO, error)
	FindNoSymbolDenoms(createAt int64, limit int64) (entity.IBCDenomList, error)
	FindSymbolDenoms() (entity.IBCDenomList, error)
	GetBaseDenomNoSymbol() ([]*dto.GetBaseDenomFromIbcDenomDTO, error)
	Count() (int64, error)
	BasedDenomCount() (int64, error)
	LatestCreateAt() (int64, error)
	UpdateSymbol(bulk *qmgo.Bulk, chain, denom, symbol string) error
	Insert(denom *entity.IBCDenom) error
	InsertBatch(denoms entity.IBCDenomList) error
	UpdateDenom(bulk *qmgo.Bulk, denom *entity.IBCDenom) error
	UpdateHops(bulk *qmgo.Bulk, chain, denom string, hops int) error
	Bulk() *qmgo.Bulk
}

var _ IDenomRepo = new(DenomRepo)

type DenomRepo struct {
}

func (repo *DenomRepo) coll() *qmgo.Collection {
	return mgo.Database(ibcDatabase).Collection(entity.IBCDenom{}.CollectionName(false))
}
func (repo *DenomRepo) Bulk() *qmgo.Bulk {
	return repo.coll().Bulk()
}

func (repo *DenomRepo) FindAll(createAt int64, limit int64) (entity.IBCDenomList, error) {
	query := bson.M{"create_at": bson.M{operator.Gt: createAt}}
	var res entity.IBCDenomList
	err := repo.coll().Find(context.Background(), query).Sort("create_at").Limit(limit).All(&res)
	return res, err
}

func (repo *DenomRepo) FindBaseDenom() (entity.IBCDenomList, error) {
	var res entity.IBCDenomList
	err := repo.coll().Find(context.Background(), bson.M{"is_base_denom": true}).All(&res)
	return res, err
}

func (repo *DenomRepo) FindByBaseDenom(baseDenom, baseDenomChain string) (entity.IBCDenomList, error) {
	var res entity.IBCDenomList
	err := repo.coll().Find(context.Background(), bson.M{"base_denom": baseDenom, "base_denom_chain": baseDenomChain}).All(&res)
	return res, err
}

func (repo *DenomRepo) FindByChain(chain string) (entity.IBCDenomList, error) {
	var res entity.IBCDenomList
	err := repo.coll().Find(context.Background(), bson.M{"chain": chain}).All(&res)
	return res, err
}

func (repo *DenomRepo) FindByDenom(denom string) (entity.IBCDenomList, error) {
	var res entity.IBCDenomList
	err := repo.coll().Find(context.Background(), bson.M{"denom": denom}).All(&res)
	return res, err
}

func (repo *DenomRepo) FindByDenomChain(denom, chain string) (*entity.IBCDenom, error) {
	var res *entity.IBCDenom
	err := repo.coll().Find(context.Background(), bson.M{"denom": denom, "chain": chain}).One(&res)
	return res, err
}

func (repo *DenomRepo) GetDenomGroupByChain() ([]*dto.GetDenomGroupByChainDTO, error) {
	group := bson.M{
		"$group": bson.M{
			"_id": "$chain",
			"denom": bson.M{
				"$addToSet": "$denom",
			},
		},
	}

	var pipe []bson.M
	pipe = append(pipe, group)
	var res []*dto.GetDenomGroupByChainDTO
	ctx, cancel := context.WithTimeout(context.Background(), constant.AggrateTimeout*time.Second)
	defer cancel()
	err := repo.coll().Aggregate(ctx, pipe).All(&res)
	return res, err
}

func (repo *DenomRepo) FindNoSymbolDenoms(createAt int64, limit int64) (entity.IBCDenomList, error) {
	var res entity.IBCDenomList
	query := bson.M{"symbol": "", "create_at": bson.M{operator.Gt: createAt}}
	err := repo.coll().Find(context.Background(), query).
		Sort("create_at").Limit(limit).All(&res)
	return res, err
}

func (repo *DenomRepo) FindSymbolDenoms() (entity.IBCDenomList, error) {
	var res entity.IBCDenomList
	err := repo.coll().Find(context.Background(), bson.M{"symbol": bson.M{"$ne": ""}}).All(&res)
	return res, err
}

func (repo *DenomRepo) UpdateSymbol(bulk *qmgo.Bulk, chain, denom, symbol string) error {
	bulk.UpdateOne(bson.M{"chain": chain, "denom": denom}, bson.M{
		"$set": bson.M{
			"symbol": symbol,
		}})
	return nil
}

func (repo *DenomRepo) Insert(denom *entity.IBCDenom) error {
	_, err := repo.coll().InsertOne(context.Background(), denom)
	return err
}

func (repo *DenomRepo) InsertBatch(denoms entity.IBCDenomList) error {
	_, err := repo.coll().InsertMany(context.Background(), denoms, insertIgnoreErrOpt)
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}

	return err
}

func (repo *DenomRepo) UpdateDenom(bulk *qmgo.Bulk, denom *entity.IBCDenom) error {
	bulk.UpdateOne(bson.M{"chain": denom.Chain, "denom": denom.Denom}, bson.M{
		"$set": bson.M{
			"base_denom":       denom.BaseDenom,
			"base_denom_chain": denom.BaseDenomChain,
			"prev_denom":       denom.PrevDenom,
			"prev_chain":       denom.PrevChain,
			"is_base_denom":    denom.IsBaseDenom,
		},
	})
	return nil
}

func (repo *DenomRepo) LatestCreateAt() (int64, error) {
	var res entity.IBCDenom
	err := repo.coll().Find(context.Background(), bson.M{}).Sort("-create_at").One(&res)
	if err != nil {
		return 0, err
	}
	return res.CreateAt, nil
}

func (repo *DenomRepo) Count() (int64, error) {
	return repo.coll().Find(context.Background(), bson.M{}).Count()
}

func (repo *DenomRepo) BasedDenomCount() (int64, error) {
	return repo.coll().Find(context.Background(), bson.M{"is_base_denom": true}).Count()
}

func (repo *DenomRepo) GetBaseDenomNoSymbol() ([]*dto.GetBaseDenomFromIbcDenomDTO, error) {
	match := bson.M{
		"$match": bson.M{
			"symbol": "",
		},
	}
	group := bson.M{
		"$group": bson.M{
			"_id": "$base_denom",
		},
	}

	var pipe []bson.M
	pipe = append(pipe, match, group)
	var res []*dto.GetBaseDenomFromIbcDenomDTO
	ctx, cancel := context.WithTimeout(context.Background(), constant.AggrateTimeout*time.Second)
	defer cancel()
	err := repo.coll().Aggregate(ctx, pipe).All(&res)
	return res, err
}

func (repo *DenomRepo) UpdateHops(bulk *qmgo.Bulk, chain, denom string, hops int) error {
	bulk.UpdateOne(bson.M{"chain": chain, "denom": denom}, bson.M{
		"$set": bson.M{
			"ibc_hops": hops,
		}})
	return nil
}
