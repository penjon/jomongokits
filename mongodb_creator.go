package mongokits

import (
	"errors"
	"fmt"
)

type MongodbCreator struct {
	databases map[string]*MongodbNode
	database  *MongodbDatabase
	ops       *MongoOptions
}

func (m *MongodbCreator) getDatabaseById(id string) (*MongodbDatabase, error) {
	if nil != m.databases {
		db, exists := m.databases[id]
		if !exists {
			return nil, fmt.Errorf("database %s no exists", id)
		}
		return db.database, nil
	}
	return m.getDatabase()
}

func (m *MongodbCreator) getDatabase() (*MongodbDatabase, error) {
	if m.database == nil {
		if nil == m.ops {
			return nil, errors.New("mongodb options required")
		}
		client, err := GetClientByOptions(m.ops)
		if nil != err {
			return nil, err
		}
		db := &MongodbDatabase{
			client:  client,
			options: m.ops,
		}
		m.database = db
	}

	return m.database, nil
}

func NewMongodbCreator(ops ...*MongoOptions) *MongodbCreator {
	creator := &MongodbCreator{}
	//多个数据源
	creator.databases = make(map[string]*MongodbNode)
	for _, op := range ops {
		client, err := GetClientByOptions(op)
		if nil != err {
			panic(err)
		}
		node := &MongodbNode{
			ops: op,
			database: &MongodbDatabase{
				client:  client,
				options: op,
			},
		}
		creator.databases[op.Id] = node
		if "default" == op.Id {
			creator.database = node.database
			creator.ops = node.ops
		}
	}

	return creator
}
