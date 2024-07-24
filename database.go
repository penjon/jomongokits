package mongokits

import (
	"errors"
)

type defaultManager struct {
	factory DatabaseFactory
}

func (m *defaultManager) SetDatabaseFactory(df DatabaseFactory) {
	m.factory = df
}

func (m *defaultManager) GetDatabase() (*MongodbDatabase, error) {
	if m.factory == nil {
		return nil, errors.New("database create factory is nil,Please setup")
	}
	return m.factory.getDatabase()
}

func (m *defaultManager) GetDatabaseById(id string) (*MongodbDatabase, error) {
	if m.factory == nil {
		return nil, errors.New("database create factory is nil,Please setup")
	}
	return m.factory.getDatabaseById(id)
}

var defMgr *defaultManager

func GetDefaultManager() DatabaseManager {
	if nil == defMgr {
		defMgr = &defaultManager{}
	}
	return defMgr
}
