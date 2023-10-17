package clients

import (
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

var (
	producerConns sync.Map
	mu            sync.Mutex
)

func InitProducer(kafka_addr string) error {
	producerconfig := sarama.NewConfig()
	producerconfig.Producer.RequiredAcks = 1        // 发送完数据需要leader和follow都确认
	producerconfig.Producer.Return.Successes = true // 成功交付的消息将在success channel返回
	producer, err := sarama.NewAsyncProducer([]string{kafka_addr}, producerconfig)
	if err != nil {
		return fmt.Errorf("init kafka producer error: %v", err)
	}
	log.Println("producer" + kafka_addr + "init success")
	producerConns.Store(kafka_addr, producer)
	return nil
}

func GetProducer(kafka_addr string) sarama.AsyncProducer {
	if v, ok := producerConns.Load(kafka_addr); !ok {
		mu.Lock()
		defer mu.Unlock()
		//拿了锁发现还没有线程初始化，那么就初始化
		if _, ok := producerConns.Load(kafka_addr); !ok {
			err := InitProducer(kafka_addr)
			if err != nil {
				panic(err)
			}
			v, _ := producerConns.Load(kafka_addr)
			return v.(sarama.AsyncProducer)
		}
	} else {
		return v.(sarama.AsyncProducer)
	}
	//等待锁的时候已经被其他线程初始化了，直接查询然后返回
	if v, ok := producerConns.Load(kafka_addr); !ok {
		panic("producer init error")
	} else {
		return v.(sarama.AsyncProducer)
	}
}

func InitConsumer(kafka_addr string) (sarama.Consumer, error) {
	consumer, err := sarama.NewConsumer([]string{kafka_addr}, nil)
	if err != nil {
		return nil, fmt.Errorf("init kafka consumer error: %v", err)
	}

	return consumer, nil
}
