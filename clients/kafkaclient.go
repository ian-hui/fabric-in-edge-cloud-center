package clients

import (
	"fmt"
	"sync"

	"github.com/IBM/sarama"
)

var (
	producerConns *sarama.AsyncProducer
	mu            = sync.Mutex{}
)

func InitProducer(kafka_addr string) error {
	producerconfig := sarama.NewConfig()
	producerconfig.Producer.RequiredAcks = 1        // 发送完数据需要leader和follow都确认
	producerconfig.Producer.Return.Successes = true // 成功交付的消息将在success channel返回
	producer, err := sarama.NewAsyncProducer([]string{kafka_addr}, producerconfig)
	if err != nil {
		return fmt.Errorf("init kafka producer error: %v", err)
	}
	fmt.Println("producer" + kafka_addr + "init success")
	producerConns = &producer
	return nil
}

func GetProducer(kafka_addr string) (sarama.AsyncProducer, error) {
	if producerConns == nil {
		mu.Lock()
		defer mu.Unlock()
		if producerConns == nil {
			err := InitProducer(kafka_addr)
			if err != nil {
				panic(err) 
			}
		}
	}
	return *producerConns, nil
}

func InitConsumer(kafka_addr string) (sarama.Consumer, error) {
	consumer, err := sarama.NewConsumer([]string{kafka_addr}, nil)
	if err != nil {
		return nil, fmt.Errorf("init kafka consumer error: %v", err)
	}

	return consumer, nil
}
