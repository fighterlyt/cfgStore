package store

import (
	"context"
	"fmt"
	"github.com/fighterlyt/cfgStore/model"
	"github.com/globalsign/mgo/bson"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

type MongoStore struct {
	client        *mongo.Client
	collection    *mongo.Collection
	latestVersion map[string]*model.Config
}

func NewMongoStore(address, db, collection string) (*MongoStore, error) {
	ctx := context.TODO()
	option := options.Client().ApplyURI(address)
	option = option.SetAppName("configManager")
	if client, err := mongo.Connect(ctx, option); err != nil {
		return nil, err
	} else {
		return &MongoStore{
			client:        client,
			collection:    client.Database(db).Collection(collection),
			latestVersion: make(map[string]*model.Config, 100),
		}, nil
	}
}

func (m *MongoStore) Init() error {
	cfgs := []*model.Config{}

	pipeline := bsonx.Arr{
		bsonx.Document(
			bsonx.Doc{{"$group", bsonx.Document(bsonx.Doc{{"_id", bsonx.String("$key")}, {"version", bsonx.Document(bsonx.Doc{{"$max", bsonx.String("$version")}})}})}},
		),
	}

	if cursor, err := m.collection.Aggregate(context.Background(), pipeline, options.Aggregate().SetComment("寻找最新版本号")); err != nil {
		return errors.Wrap(err, "初始化")
	} else {
		ctx := context.Background()
		defer cursor.Close(ctx)
		for cursor.Next(ctx) {
			elem := &VersionInfo{}
			if err := cursor.Decode(elem); err != nil {
				return errors.Wrap(err, "获取最后版本")
			} else {
				cfg := &model.Config{}
				if err = m.collection.FindOne(context.Background(), bson.M{
					"key":     elem.Id,
					"version": elem.Version,
				}).Decode(cfg); err != nil {
					return errors.Wrap(err, "加载数据")
				} else {
					cfgs = append(cfgs, cfg)
				}
			}
		}
	}
	for _, cfg := range cfgs {
		m.latestVersion[cfg.Key] = cfg
	}
	return nil
}

func (m *MongoStore) Write(key string, data string, cfgType model.CfgType) error {
	version := 1
	if old, exist := m.latestVersion[key]; exist {
		version = old.Version + 1
	}
	cfg := model.NewConfig(data, key, cfgType, version)
	if err := m.writeToMongo(cfg); err == nil {
		m.latestVersion[key] = cfg
		return nil
	} else {
		return err
	}
}

func (m *MongoStore) Get(key string, version int) (data string, cfgType model.CfgType, err error) {
	if version == 0 {
		if cfg, exist := m.latestVersion[key]; exist {
			data, cfgType = cfg.Data, cfg.Type
			return
		} else {
			err = fmt.Errorf("不存在的key[%s]", key)
			return
		}
	} else {
		if cfg, findErr := m.getFromMongo(key, version); findErr != nil {
			err = findErr
			return
		} else {
			data, cfgType = cfg.Data, cfg.Type
			return
		}
	}
}
func (m *MongoStore) writeToMongo(data *model.Config) error {
	if _, err := m.collection.InsertOne(context.Background(), data); err != nil {
		return errors.Wrap(err, "插入最新数据")
	}
	return nil
}

func (m *MongoStore) getFromMongo(key string, version int) (*model.Config, error) {
	cfg := &model.Config{}
	if err := m.collection.FindOne(context.Background(), bson.M{"key": key, "version": version}).Decode(cfg); err != nil {
		return nil, errors.WithMessagef(err, "加载配置[%s][%d]", key, version)
	} else {
		return cfg, nil
	}
}

func (m *MongoStore) drop() error {
	return m.collection.Drop(context.Background())
}

type VersionInfo struct {
	Id      string `bson:"_id"`
	Version int    `bson:"version"`
}
