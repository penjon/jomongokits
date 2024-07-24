package mongokits

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"os"
	"strconv"
	"strings"
	"time"
)

type TransactionFunc func() error

var clients = make(map[string]*MongoClient)

type MongoClient struct {
	database *mongo.Database
	duration time.Duration
	options  *MongoOptions
}

/*
*
通过配置获取数据库客户端连接
*/
func GetClientByOptions(mongoOptions *MongoOptions) (*MongoClient, error) {
	if client, exists := clients[mongoOptions.Id]; !exists {
		database, err := createMongoDatabase(mongoOptions.server, mongoOptions.db, mongoOptions.timeout, mongoOptions.userName, mongoOptions.userPass)
		if err != nil {
			return nil, err
		}
		client = &MongoClient{
			database: database,
			duration: time.Duration(mongoOptions.timeout) * time.Second,
			options:  mongoOptions,
		}
		clients[mongoOptions.Id] = client
		return client, nil
	} else {
		return client, nil
	}
}

func GetClientByName(name string) (*MongoClient, error) {
	if client, exists := clients[name]; !exists {
		prefix := strings.ToUpper(name)
		server := os.Getenv(fmt.Sprintf("%s_MONGODB_SERVER", prefix))
		db := os.Getenv(fmt.Sprintf("%s_MONGODB_DB", prefix))
		userName := os.Getenv(fmt.Sprintf("%s_MONGODB_USER_NAME", prefix))
		userPass := os.Getenv(fmt.Sprintf("%s_MONGODB_USER_PASSWORD", prefix))
		timeout, _ := strconv.Atoi(os.Getenv(fmt.Sprintf("%s_MONGODB_TIMEOUT", prefix)))
		op := &MongoOptions{
			Id:       name,
			server:   server,
			db:       db,
			timeout:  timeout,
			userName: userName,
			userPass: userPass,
		}
		return GetClientByOptions(op)
	} else {
		return client, nil
	}
}

func createMongoDatabase(server string, db string, timeout int, userName string, userPassword string) (*mongo.Database, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(server))
	if err != nil {
		return nil, err
	}
	dur := time.Duration(timeout) * time.Second
	ctx, _ := context.WithTimeout(context.Background(), dur)
	if err := client.Connect(ctx); err != nil {
		return nil, err
	}
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, err
	}
	return client.Database(db), nil
}

func (client *MongoClient) GetCollection(tableName string) {
	collections := client.database.Collection(tableName)

	println(collections.Name())
}

func (client *MongoClient) Save(tableName string, table interface{}) (interface{}, error) {
	ctx := client.GetCtx()
	result, err := client.database.Collection(tableName).InsertOne(ctx, table)
	if err != nil {
		return nil, err
	}
	fmt.Println("Inserted a single document: ", result.InsertedID)
	return result.InsertedID, nil
}

func (client *MongoClient) UpdateWithTransaction(tableName string, filter bson.M, document interface{}, handlers ...TransactionFunc) error {
	//ctx := client.GetCtx()
	ctx := context.Background()
	var err error
	var session mongo.Session

	if session, err = client.database.Client().StartSession(); err != nil {
		return err
	}

	defer session.EndSession(ctx)

	if err := session.StartTransaction(); err != nil {
		return err
	}

	if err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		if err := client.database.Collection(tableName).FindOneAndReplace(sc, filter, document).Err(); err != nil {
			return err
		}

		if nil != handlers {
			for _, handler := range handlers {
				if err := handler(); err != nil {
					return err
				}
			}
		}
		if err := session.CommitTransaction(sc); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (client *MongoClient) Update(tableName string, filter bson.M, setter bson.D) error {
	ctx := client.GetCtx()
	_, err := client.database.Collection(tableName).UpdateOne(ctx, filter, setter)
	return err
}
func (client *MongoClient) FindOneAndReplace(tableName string, filter bson.M, document interface{}) error {
	ctx := client.GetCtx()
	return client.database.Collection(tableName).FindOneAndReplace(ctx, filter, document).Err()
}

func (client *MongoClient) UpdateMany(tableName string, filter bson.M, setter interface{}) error {
	ctx := client.GetCtx()
	_, err := client.database.Collection(tableName).UpdateMany(ctx, filter, setter)
	return err
}

/*
*
通过条件查询一个文档
*/
func (client *MongoClient) FindOne(tableName string, filter bson.M, table interface{}) error {
	result := client.database.Collection(tableName).FindOne(client.GetCtx(), filter)
	if result.Err() != nil {
		return result.Err()
	}
	err := result.Decode(table)
	if err != nil {
		return err
	}

	return nil
}

func (client *MongoClient) FindCount(tableName string, filter bson.M) (int64, error) {
	return client.database.Collection(tableName).CountDocuments(client.GetCtx(), filter)
}

func (client *MongoClient) Delete(tableName string, filter bson.M) error {
	_, err := client.database.Collection(tableName).DeleteOne(client.GetCtx(), filter)
	return err
}

func (client *MongoClient) GetRaw() *mongo.Database {
	return client.database
}

func (client *MongoClient) Status() (string, error) {
	bson, err := client.database.RunCommand(context.Background(), bson.M{"serverStatus": 1}).DecodeBytes()
	if err != nil {
		return "", err
	}
	return bson.String(), nil
}

/*
*
通过条件查询列表
*/
func (client *MongoClient) FindAllByCondition(tableName string, filter bson.M, options *options.FindOptions) (*mongo.Cursor, error) {
	return client.database.Collection(tableName).Find(client.GetCtx(), filter, options)
}

func (client *MongoClient) FindAll(tableName string, options *options.FindOptions) (*mongo.Cursor, error) {
	return client.FindAllByCondition(tableName, bson.M{}, options)
	//return client.database.Collection(tableName).Find(client.GetCtx(),nil)
}

func (client *MongoClient) GetCtx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), client.duration)
	return ctx
}

func (client *MongoClient) GetDuration() time.Duration {
	return client.duration
}

func (client *MongoClient) GetCountByCondition(tableName string, filter bson.M) (int64, error) {
	return client.database.Collection(tableName).CountDocuments(client.GetCtx(), filter)
}

func (client *MongoClient) GetByAggregate(tableName string, pipeline mongo.Pipeline) ([]bson.M, error) {
	cursor, err := client.database.Collection(tableName).Aggregate(client.GetCtx(), pipeline)
	if err != nil {
		return nil, err
	}
	var results []bson.M
	if err = cursor.All(client.GetCtx(), &results); err != nil {
		return nil, err
	}
	return results, nil
}
