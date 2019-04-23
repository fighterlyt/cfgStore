package model

import "go.mongodb.org/mongo-driver/bson/primitive"

type Config struct {
	Id      primitive.ObjectID `bson:"_id"`
	Data    string             `bson:"data"`    //数据
	Key     string             `bson:"key"`     //key
	Version int                `bson:"version"` //版本
	Type    CfgType            `bson:"type"`
}

func NewConfig(data string, key string, cfgType CfgType, version int) *Config {
	return &Config{
		Id:      primitive.NewObjectID(),
		Data:    data,
		Key:     key,
		Version: version,
		Type:    cfgType,
	}
}
