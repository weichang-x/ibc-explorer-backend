package repository

import (
	"context"
	"encoding/json"

	"testing"

	conf "github.com/irisnet/ibc-explorer-backend/internal/app/config"
)

func TestMain(m *testing.M) {
	InitMgo(conf.Mongo{
		//Url:      "mongodb://username:password@host:port/?authSource=database",
		//Database: "ibc_explorer",
		Url:      "mongodb://username:password@host:port/?authSource=database",
		Database: "ibc_explorer",
	}, context.Background())
	m.Run()
}

func TestChainConfigRepo_FindAll(t *testing.T) {
	data, err := new(ChainConfigRepo).FindAll()
	if err != nil {
		t.Fatal(err.Error())
	}
	ret, _ := json.Marshal(data)
	t.Log(string(ret))
}

func TestChainConfigRepo_FindOne(t *testing.T) {
	t.Log(ibcDatabase)
	data, err := new(ChainConfigRepo).FindOne("irishub_qa")
	if err != nil {
		t.Fatal(err.Error())
	}
	ret, _ := json.Marshal(data)
	t.Log(string(ret))
}
