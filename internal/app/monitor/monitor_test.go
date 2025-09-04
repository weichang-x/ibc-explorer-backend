package monitor

import (
	"context"
	"testing"

	conf "github.com/irisnet/ibc-explorer-backend/internal/app/config"
	"github.com/irisnet/ibc-explorer-backend/internal/app/repository"
)

func TestMain(m *testing.M) {
	repository.InitMgo(conf.Mongo{
		Url:      "mongodb://username:password@host:port/?authSource=database",
		Database: "ibc_explorer",
	}, context.Background())
	m.Run()
}

func Test_checkLcd(t *testing.T) {
	value := checkLcd("https://emoney.validator.network/api", "/ibc/core/channel/v1beta1/channels?pagination.offset=OFFSET&pagination.limit=LIMIT&pagination.count_total=true")
	if value {
		t.Log("pass")
	}
}
