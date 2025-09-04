package repository

import (
	"testing"

	"github.com/irisnet/ibc-explorer-backend/internal/app/utils"
)

func TestIbcRelayerRepo_FindAllRelayerForCache(t *testing.T) {
	data, err := new(IbcRelayerRepo).FindAllRelayerForCache()
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(string(utils.MarshalJsonIgnoreErr(data)))
}
