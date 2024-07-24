package mongokits

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongodbGenericComplex[T Table] struct {
	writer *MongodbDatabase
	reader *MongodbDatabase
}

func GetGenericComplexDatabase[T Table](writerId string, readerId string) (*MongodbGenericComplex[T], error) {
	writer, err := GetDefaultManager().GetDatabaseById(writerId)
	if nil != err {
		return nil, err
	}

	reader, err := GetDefaultManager().GetDatabaseById(readerId)
	if nil != err {
		return nil, err
	}

	return &MongodbGenericComplex[T]{
		writer: writer,
		reader: reader,
	}, nil
}

func (i *MongodbGenericComplex[T]) GetWriterRaw() *mongo.Database {
	return i.writer.GetRaw()
}
func (i *MongodbGenericComplex[T]) GetReaderRaw() *mongo.Database {
	return i.reader.GetRaw()
}

func (i *MongodbGenericComplex[T]) Count(filter bson.M) (int64, error) {
	var r T
	return i.reader.GetCountByCondition(r.TableName(), filter)
}

func (i *MongodbGenericComplex[T]) InsertAll(tables []interface{}) (int, []interface{}, error) {
	var r T
	result, err := i.writer.GetRaw().Collection(r.TableName()).InsertMany(context.TODO(), tables)
	if err != nil {
		return 0, nil, err
	}
	totalCount := len(tables)
	insertCount := len(result.InsertedIDs)
	if insertCount != totalCount {
		return insertCount, nil, fmt.Errorf("insert failre,%d/%d", insertCount, totalCount)
	}

	return insertCount, result.InsertedIDs, nil
}

func (i *MongodbGenericComplex[T]) Insert(table Table) (string, error) {
	instanceId, err := i.writer.Save(table)
	if err != nil {
		return "", err
	}
	_id, ok := instanceId.(primitive.ObjectID)

	if ok {
		return _id.Hex(), nil
	}

	_string, ok := instanceId.(string)
	if ok {
		return _string, nil
	}
	return "", nil
}

func (i *MongodbGenericComplex[T]) QueryByCond(cond interface{}, op *options.FindOptions) ([]T, error) {
	var result []T
	var r T
	return result, i.reader.QueryAllByCondition(r.TableName(), cond, op, &result)
}

func (i *MongodbGenericComplex[T]) GetAll(page *Page) ([]T, error) {
	var result []T
	var r T

	op := &options.FindOptions{}
	if nil != page && page.Page > 0 && page.PageSize > 0 {
		ps := int64(page.PageSize)
		skip := int64(page.PageSize * (page.Page - 1))
		op.Limit = &ps
		op.Skip = &skip
	}
	return result, i.reader.QueryAllByCondition(r.TableName(), bson.M{}, op, &result)
}

func (i *MongodbGenericComplex[T]) GetAllByCond(cond map[string]interface{}, page *Page) ([]T, error) {
	c := primitive.M(cond)
	op := &options.FindOptions{}
	if nil != page && page.Page > 0 && page.PageSize > 0 {
		ps := int64(page.PageSize)
		skip := int64(page.PageSize * (page.Page - 1))
		op.Limit = &ps
		op.Skip = &skip
	}
	var result []T
	var r T
	return result, i.reader.QueryAllByCondition(r.TableName(), c, op, &result)
}

func (i *MongodbGenericComplex[T]) GetByCond(cond bson.M, op *options.FindOptions) (T, error) {
	var r T
	result, err := i.QueryByCond(cond, op)
	if err != nil {
		return r, nil
	}

	if len(result) == 0 {
		return r, nil
	}
	return result[0], nil
}

func (i *MongodbGenericComplex[T]) GetById(id string) (T, error) {
	var r T
	oid, _ := primitive.ObjectIDFromHex(id)
	result, err := i.QueryByCond(bson.M{
		"_id": oid,
	}, &options.FindOptions{})
	if err != nil {
		return r, err
	}

	if len(result) == 0 {
		var z T
		return z, nil
	}
	return result[0], nil
}

func (i *MongodbGenericComplex[T]) Update(doc T) error {
	database, err := GetDefaultManager().GetDatabase()
	if err != nil {
		return err
	}
	//oid, _ := primitive.ObjectIDFromHex(doc.PrimaryKey())
	return database.Update(doc, bson.M{"_id": doc.PrimaryKey()})
}

func (i *MongodbGenericComplex[T]) UpdateAll(tables []T) (int64, int64, error) {
	var r T
	var writers []mongo.WriteModel
	for _, table := range tables {
		writers = append(writers, mongo.NewReplaceOneModel().SetFilter(bson.M{table.PrimaryKeyName(): table.PrimaryKey()}).SetReplacement(table).SetUpsert(true))
	}
	result, err := i.writer.GetRaw().Collection(r.TableName()).BulkWrite(context.TODO(), writers, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, 0, err
	}
	return result.ModifiedCount, result.InsertedCount, nil
}

func (i *MongodbGenericComplex[T]) UpdateSet(cond bson.M, setter bson.M) error {
	var r T
	if _, err := i.writer.GetRaw().Collection(r.TableName()).UpdateOne(context.TODO(), cond, setter); err != nil {
		return err
	}
	return nil
}

func (i *MongodbGenericComplex[T]) Delete(ids ...string) error {
	var r T
	var oid []primitive.ObjectID
	for _, id := range ids {
		o, _ := primitive.ObjectIDFromHex(id)
		oid = append(oid, o)
	}
	_, err := i.writer.GetRaw().Collection(r.TableName()).DeleteMany(context.TODO(), bson.M{"_id": bson.M{"$in": oid}})
	return err
}
