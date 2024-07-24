package mongokits

import (
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Page struct {
	Page     uint
	PageSize uint
}

var (
	ErrorDocumentNotFound = errors.New("data record not found")
)

type MongodbNode struct {
	ops      *MongoOptions
	database *MongodbDatabase
}

type MongodbDatabase struct {
	client  *MongoClient
	options *MongoOptions
}

func (i *MongodbDatabase) Status() (string, error) {
	return i.client.Status()
}

func (i *MongodbDatabase) Save(table Table, handlers ...TransactionFunc) (interface{}, error) {
	return i.client.Save(table.TableName(), table)
}

func (i *MongodbDatabase) GetRaw() *mongo.Database {

	return i.client.GetRaw()
}

func (i *MongodbDatabase) Update(table Table, condition interface{}, handlers ...TransactionFunc) error {
	tbName := table.TableName()
	if nil != handlers {
		return i.client.UpdateWithTransaction(tbName, condition.(bson.M), table, handlers...)
	} else {
		return i.client.FindOneAndReplace(tbName, condition.(bson.M), table)
	}
}

func (i *MongodbDatabase) Query(tableName string, condition interface{}, result interface{}) error {
	cur, err := i.client.FindAllByCondition(tableName, condition.(bson.M), &options.FindOptions{})
	if nil != err {
		return err
	}

	ctx := i.client.GetCtx()
	defer cur.Close(ctx)
	return cur.All(i.client.GetCtx(), result)
	//return mongo.client.FindAllByCondition(tableName,condition.(bson.M),&options.FindOptions{})
}

func (i *MongodbDatabase) QueryAll(tableName string, result interface{}) error {
	cur, err := i.client.FindAll(tableName, &options.FindOptions{})
	if nil != err {
		return err
	}

	ctx := i.client.GetCtx()
	defer cur.Close(ctx)
	return cur.All(i.client.GetCtx(), result)
}

func (i *MongodbDatabase) QueryOne(tableName string, condition interface{}, result interface{}) error {
	return i.client.FindOne(tableName, condition.(bson.M), result)
}

func (i *MongodbDatabase) QueryByDocumentId(tableName string, docId string, result interface{}) error {
	objectId, err := primitive.ObjectIDFromHex(docId)
	if err != nil {
		return err
	}
	return i.client.FindOne(tableName, bson.M{"_id": objectId}, result)
}

func (i *MongodbDatabase) QueryAllByCondition(tableName string, condition interface{}, findOption *options.FindOptions, result interface{}) error {
	cur, err := i.client.FindAllByCondition(tableName, condition.(bson.M), findOption)
	if nil != err {
		return err
	}
	ctx := i.client.GetCtx()
	defer cur.Close(ctx)
	return cur.All(i.client.GetCtx(), result)
}

func (i *MongodbDatabase) GetCountByCondition(tableName string, condition interface{}) (int64, error) {
	return i.client.GetCountByCondition(tableName, condition.(bson.M))
}
