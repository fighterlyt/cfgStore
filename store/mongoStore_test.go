package store

import (
	"fmt"
	"github.com/fighterlyt/cfgStore/model"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewMongoStore(t *testing.T) {
	_, err := NewMongoStore("mongodb://my_user:password123@localhost:27018/test", "test", "test")
	require.NoError(t, err)
}

func TestMongoStore_Write(t *testing.T) {
	s, err := NewMongoStore("mongodb://my_user:password123@localhost:27018/orderbook", "test", "test")
	require.NoError(t, err)
	require.NoError(t, s.drop())
	require.NoError(t, s.Init())

	count := 2
	times := 5
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("times%d", i)
		for j := 0; j < times; j++ {
			data := fmt.Sprintf("data%d", j)
			require.NoError(t, s.Write(key, data, model.JSON))

		}
	}

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("times%d", i)

		for j := 0; j < times; j++ {
			data := fmt.Sprintf("data%d", j)
			getData, cfgType, err := s.Get(key, j+1)
			require.NoError(t, err)
			require.EqualValues(t, getData, data)
			require.EqualValues(t, cfgType, model.JSON)
		}
	}

}
