package mongokits

type Table interface {
	TableName() string
	PrimaryKey() interface{}
	PrimaryKeyName() string
}

type DatabaseManager interface {
	GetDatabase() (*MongodbDatabase, error)
	GetDatabaseById(id string) (*MongodbDatabase, error)
	SetDatabaseFactory(factory DatabaseFactory)
}

type DatabaseFactory interface {
	getDatabase() (*MongodbDatabase, error)
	getDatabaseById(id string) (*MongodbDatabase, error)
}
