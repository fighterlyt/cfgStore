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
	"sync"
)

//MongoStore 基于Mongo的存储
type MongoStore struct {
	client        *mongo.Client
	collection    *mongo.Collection
	latestVersion map[string]*model.Config
	locks         map[string]*sync.RWMutex
	globalLock    *sync.RWMutex
}

/*NewMongoStore 构建一个MongoStore
参数:
*	address   	string	mongo服务器地址 mongodb://用户名:密码@ip:端口/验证服务器
*	db        	string	数据库名
*	collection	string	表名
返回值:
*	*MongoStore	*MongoStore
*	error      	error
*/
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
			globalLock:    &sync.RWMutex{},
			locks:         make(map[string]*sync.RWMutex, 100),
		}, nil
	}
}

/*Init 初始化
参数:
返回值:
*	error	error
*/

func (m *MongoStore) Init() error {
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
					m.latestVersion[cfg.Key] = cfg
					m.locks[cfg.Key] = &sync.RWMutex{}
				}
			}
		}
	}

	return nil
}

/*Write 更新配置
参数:
*	key    	string			key
*	data   	string			数据
*	cfgType	model.CfgType	配置类型
返回值:
*	error	error
*/

func (m *MongoStore) Write(key string, data string, cfgType model.CfgType) error {
	m.globalLock.Lock()
	defer m.globalLock.Unlock()

	version := 1
	if old, exist := m.latestVersion[key]; exist {
		version = old.Version + 1
	} else {
		m.locks[key] = &sync.RWMutex{}
	}
	m.locks[key].Lock()
	m.locks[key].Unlock()

	cfg := model.NewConfig(data, key, cfgType, version)
	if err := m.writeToMongo(cfg); err == nil {
		m.latestVersion[key] = cfg
		return nil
	} else {
		return err
	}
}

/*Get 获取配置
参数:
*	key    	string			key
*	version	int				版本，0表示最新版本
返回值:
*	data   	string			配置数据
*	cfgType	model.CfgType	配置类型
*	err    	error
*/
func (m *MongoStore) Get(key string, version int) (data string, cfgType model.CfgType, err error) {
	m.globalLock.RLock()
	defer m.globalLock.RUnlock()

	if lock, exist := m.locks[key]; exist {
		lock.RLock()
		defer lock.RUnlock()
	} else {
		err = fmt.Errorf("不存在的key[%s]", key)
	}

	if version == 0 {
		cfg := m.latestVersion[key]
		data, cfgType = cfg.Data, cfg.Type
		return

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

/*writeToMongo 写入数据到mongo
参数:
*	data	*model.Config
返回值:
*	error	error
*/
func (m *MongoStore) writeToMongo(data *model.Config) error {
	if _, err := m.collection.InsertOne(context.Background(), data); err != nil {
		return errors.Wrap(err, "插入最新数据")
	}
	return nil
}

/*getFromMongo 从mongo读取数据
参数:
*	key    	string		key
*	version	int			版本
返回值:
*	*model.Config	*model.Config
*	error        	error
*/
func (m *MongoStore) getFromMongo(key string, version int) (*model.Config, error) {
	cfg := &model.Config{}
	if err := m.collection.FindOne(context.Background(), bson.M{"key": key, "version": version}).Decode(cfg); err != nil {
		return nil, errors.WithMessagef(err, "加载配置[%s][%d]", key, version)
	} else {
		return cfg, nil
	}
}

/*drop 清除数据，用于测试
参数:
返回值:
*	error	error
*/
func (m *MongoStore) drop() error {
	return m.collection.Drop(context.Background())
}

//VersionInfo 版本信息
type VersionInfo struct {
	Id      string `bson:"_id"`     //key
	Version int    `bson:"version"` //最新版本
}
