package mongokits

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Server       string `env:"MONGODB_SERVER"`
	Database     string `env:"MONGODB_NAME"`
	Timeout      int    `env:"MONGODB_TIMEOUT" env_default:"5"`
	UserName     string `env:"MONGODB_USERNAME"`
	UserPassword string `env:"MONGODB_PASSWORD"`
	Id           string `env:"MONGODB_ID" env_default:"default"`
}

func (m *Config) Parse(prefix string) error {
	pre := ""
	if len(prefix) > 0 {
		pre = fmt.Sprintf("%s_", strings.ToUpper(pre))
	}
	m.Server = os.Getenv(pre + "MONGODB_SERVER")
	m.Database = os.Getenv(pre + "MONGODB_NAME")
	m.UserName = os.Getenv(pre + "MONGODB_USERNAME")
	m.UserPassword = os.Getenv(pre + "MONGODB_PASSWORD")
	m.Id = os.Getenv(pre + "MONGODB_ID")
	to, err := strconv.Atoi(os.Getenv(pre + "MONGODB_TIMEOUT"))

	fmt.Println(fmt.Sprintf("Mongodb Server[%s] Database[%s]", m.Server, m.Database))
	if err != nil {
		to = 5
	}
	m.Timeout = to
	return nil
}
