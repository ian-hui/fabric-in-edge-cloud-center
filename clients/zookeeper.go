package clients

import (
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	ZookeeperConns *zk.Conn
)

func InitZookeeper() {
	conn, _, err := zk.Connect([]string{"localhost:2181"}, time.Second)
	if err != nil {
		panic(err)
	}
	ZookeeperConns = conn
	fmt.Println("zookeeper init success")
}
