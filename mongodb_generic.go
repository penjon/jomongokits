package mongokits

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongodbGeneric[T Table] struct {
	database *MongodbDatabase
}

func GetGenericDatabase[T Table]() (*MongodbGeneric[T], error) {
	db, err := GetDefaultManager().GetDatabase()
	if nil != err {
		return nil, err
	}
	return &MongodbGeneric[T]{
		database: db,
	}, nil
}
func (i *MongodbGeneric[T]) GetRaw() *mongo.Database {
	return i.database.GetRaw()
}
func GetGenericDatabaseById[T Table](dbId string) (*MongodbGeneric[T], error) {
	db, err := GetDefaultManager().GetDatabaseById(dbId)
	if nil != err {
		return nil, err
	}
	return &MongodbGeneric[T]{
		database: db,
	}, nil
}
func (i *MongodbGeneric[T]) Count(filter bson.M) (int64, error) {
	var r T
	return i.database.GetCountByCondition(r.TableName(), filter)
}

func (i *MongodbGeneric[T]) InsertAll(tables []interface{}) (int, []interface{}, error) {
	var r T
	result, err := i.database.GetRaw().Collection(r.TableName()).InsertMany(context.TODO(), tables)
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

func (i *MongodbGeneric[T]) Insert(table Table) (string, error) {
	instanceId, err := i.database.Save(table)
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

func (i *MongodbGeneric[T]) QueryByCond(cond interface{}, op *options.FindOptions) ([]T, error) {
	var result []T
	var r T
	return result, i.database.QueryAllByCondition(r.TableName(), cond, op, &result)
}

func (i *MongodbGeneric[T]) GetAll(page *Page) ([]T, error) {
	var result []T
	var r T

	op := &options.FindOptions{}
	if nil != page && page.Page > 0 && page.PageSize > 0 {
		ps := int64(page.PageSize)
		skip := int64(page.PageSize * (page.Page - 1))
		op.Limit = &ps
		op.Skip = &skip
	}
	return result, i.database.QueryAllByCondition(r.TableName(), bson.M{}, op, &result)
}

func (i *MongodbGeneric[T]) GetAllByCond(cond map[string]interface{}, page *Page) ([]T, error) {
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
	return result, i.database.QueryAllByCondition(r.TableName(), c, op, &result)
}

func (i *MongodbGeneric[T]) GetByCond(cond bson.M, op *options.FindOptions) (T, error) {
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

func (i *MongodbGeneric[T]) GetById(id string) (T, error) {
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

func (i *MongodbGeneric[T]) Update(doc T) error {
	database, err := GetDefaultManager().GetDatabase()
	if err != nil {
		return err
	}
	//oid, _ := primitive.ObjectIDFromHex(doc.PrimaryKey())
	return database.Update(doc, bson.M{"_id": doc.PrimaryKey()})
}

func (i *MongodbGeneric[T]) UpdateAll(tables []Table) (int64, int64, error) {
	database, err := GetDefaultManager().GetDatabase()
	if err != nil {
		return 0, 0, err
	}
	var r T
	var writers []mongo.WriteModel
	for _, table := range tables {
		writers = append(writers, mongo.NewReplaceOneModel().SetFilter(bson.M{table.PrimaryKeyName(): table.PrimaryKey()}).SetReplacement(table).SetUpsert(true))
	}
	result, err := database.GetRaw().Collection(r.TableName()).BulkWrite(context.TODO(), writers, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, 0, err
	}
	return result.ModifiedCount, result.InsertedCount, nil
}

func (i *MongodbGeneric[T]) UpdateSet(cond bson.M, setter bson.M) error {
	var r T
	if _, err := i.database.GetRaw().Collection(r.TableName()).UpdateOne(context.TODO(), cond, setter); err != nil {
		return err
	}
	return nil
}

func (i *MongodbGeneric[T]) Delete(ids ...string) error {
	var r T
	var oid []primitive.ObjectID
	for _, id := range ids {
		o, _ := primitive.ObjectIDFromHex(id)
		oid = append(oid, o)
	}
	_, err := i.database.GetRaw().Collection(r.TableName()).DeleteMany(context.TODO(), bson.M{"_id": bson.M{"$in": oid}})
	return err
}

// InsertAll 批量新增数据,返回参数int = 新增数量, []interface{}=写入数据ID, error=异常
func InsertAll[T Table](tables []interface{}) (int, []interface{}, error) {
	database, err := GetDefaultManager().GetDatabase()
	if err != nil {
		return 0, nil, err
	}
	var r T
	result, err := database.GetRaw().Collection(r.TableName()).InsertMany(context.TODO(), tables)
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

func Insert(table Table) (string, error) {
	database, err := GetDefaultManager().GetDatabase()
	if err != nil {
		return "", err
	}

	instanceId, err := database.Save(table)
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

func QueryByCond[T Table](cond interface{}, op *options.FindOptions) ([]T, error) {
	database, err := GetDefaultManager().GetDatabase()
	if err != nil {
		return nil, err
	}
	var result []T
	var r T
	return result, database.QueryAllByCondition(r.TableName(), cond, op, &result)
}

func GetAll[T Table](page *Page) ([]T, error) {
	database, err := GetDefaultManager().GetDatabase()
	if err != nil {
		return nil, err
	}
	var result []T
	var r T

	op := &options.FindOptions{}
	if nil != page && page.Page > 0 && page.PageSize > 0 {
		ps := int64(page.PageSize)
		skip := int64(page.PageSize * (page.Page - 1))
		op.Limit = &ps
		op.Skip = &skip
	}
	return result, database.QueryAllByCondition(r.TableName(), bson.M{}, op, &result)
}

func GetAllByCond[T Table](cond map[string]interface{}, page *Page) ([]T, error) {
	database, err := GetDefaultManager().GetDatabase()
	if err != nil {
		return nil, err
	}
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
	return result, database.QueryAllByCondition(r.TableName(), c, op, &result)
}

func GetByCond[T Table](cond bson.M, op *options.FindOptions) (T, error) {
	var r T
	result, err := QueryByCond[T](cond, op)
	if err != nil {
		return r, nil
	}

	if len(result) == 0 {
		return r, nil
	}
	return result[0], nil
}

func GetById[T Table](id string) (T, error) {
	var r T
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return r, err
	}
	result, err := QueryByCond[T](bson.M{
		"_id": oid,
	}, &options.FindOptions{})
	if err != nil {
		return r, err
	}

	if len(result) == 0 {
		var z T
		///return r, ErrorDocumentNotFound
		return z, nil
	}
	return result[0], nil
}

func Update[T Table](doc T) error {
	database, err := GetDefaultManager().GetDatabase()
	if err != nil {
		return err
	}
	//oid, _ := primitive.ObjectIDFromHex(doc.PrimaryKey())
	return database.Update(doc, bson.M{"_id": doc.PrimaryKey()})
}

func UpdateAll[T Table](tables []Table) (int64, int64, error) {
	database, err := GetDefaultManager().GetDatabase()
	if err != nil {
		return 0, 0, err
	}
	var r T
	var writers []mongo.WriteModel
	for _, table := range tables {
		writers = append(writers, mongo.NewReplaceOneModel().SetFilter(bson.M{table.PrimaryKeyName(): table.PrimaryKey()}).SetReplacement(table).SetUpsert(true))
	}
	result, err := database.GetRaw().Collection(r.TableName()).BulkWrite(context.TODO(), writers, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, 0, err
	}
	return result.ModifiedCount, result.InsertedCount, nil
}

func UpdateSet[T Table](cond bson.M, setter bson.M) error {
	database, err := GetDefaultManager().GetDatabase()
	if err != nil {
		return err
	}
	var r T
	if _, err := database.GetRaw().Collection(r.TableName()).UpdateOne(context.TODO(), cond, setter); err != nil {
		return err
	}
	return nil
}

func Delete[T Table](ids ...string) error {
	var r T
	r.TableName()
	database, err := GetDefaultManager().GetDatabase()
	if err != nil {
		return err
	}
	var oid []primitive.ObjectID
	for _, id := range ids {
		o, _ := primitive.ObjectIDFromHex(id)
		oid = append(oid, o)
	}
	_, err = database.GetRaw().Collection(r.TableName()).DeleteMany(context.TODO(), bson.M{"_id": bson.M{"$in": oid}})
	return err
}
