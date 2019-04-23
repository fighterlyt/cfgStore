package store

import "github.com/fighterlyt/cfgStore/model"

type Store interface {
	Write(key string, data string, cfgType model.CfgType) error
	Get(key string, version int) (data string, cfgType model.CfgType, err error)
}
