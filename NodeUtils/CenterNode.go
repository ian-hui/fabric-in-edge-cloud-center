package NodeUtils

import (
	"fabric-go-sdk/clients"
	"fmt"
	"sync"

	"github.com/cloudflare/cfssl/log"

	"github.com/IBM/sarama"
)

// 初始化center-consumer
//
//topic:uploadposition,ReceiveRequest,UploadCiText
func InitCenterNode(topics []string, center_nodestru Nodestructure) {
	//init kafka producer
	clients.InitProducer(center_nodestru.KafkaAddr)
	//create db in couchdb
	cc, err := clients.GetCouchdb(center_nodestru.Couchdb_addr)
	if err != nil {
		fmt.Println("get couchdb error:", err)
	}
	if err = cc.Create_ciphertext_info(); err != nil {
		fmt.Println("create ciphertext_info db error:", err)
	}
	if err = cc.Create_position_info(); err != nil {
		fmt.Println("create cipherkey_info db error:", err)
	}

	var wg sync.WaitGroup
	wg.Add(5)
	consumer1, err := clients.InitConsumer(center_nodestru.KafkaAddr)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	go consumeUploadPosition(consumer1, center_nodestru, &wg)
	go consumeUploadCiText(consumer1, center_nodestru, &wg)
	go consumeKeyReqForwarding(consumer1, center_nodestru, &wg)
	fmt.Println(center_nodestru.KafkaAddr, "init center-consumer1 begin")
	consumer2, err := clients.InitConsumer(center_nodestru.KafkaAddr)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	go consumeFileReqestToCenter(consumer2, center_nodestru, &wg)
	go consumeUploadKeyPosition(consumer2, center_nodestru, &wg)
	fmt.Println(center_nodestru.KafkaAddr, "init center-consumer2 begin")
	wg.Wait()

}

func consumeUploadPosition(consumer sarama.Consumer, center_nodestru Nodestructure, wg *sync.WaitGroup) {
	partitonConsumer, err := consumer.ConsumePartition("uploadposition", 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	defer wg.Done()
	for {
		select {
		case msg := <-partitonConsumer.Messages():
			if err := uploadFilePosition(center_nodestru, msg.Value); err != nil {
				log.Info("uploadFilePosition error:", err)
			}
		case <-partitonConsumer.Errors():
			log.Critical("consumerUploadPosition error")
		}
	}
}

func consumeUploadCiText(consumer sarama.Consumer, center_nodestru Nodestructure, wg *sync.WaitGroup) {
	partitonConsumer, err := consumer.ConsumePartition("UploadCiText", 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	defer wg.Done()
	for {
		select {
		case msg := <-partitonConsumer.Messages():
			if err := UploadCiText(center_nodestru, msg.Value); err != nil {
				log.Info("UploadCiText error:", err)
			}
		case <-partitonConsumer.Errors():
			log.Critical("consumerUploadCiText error")
		}
	}
}

func consumeKeyReqForwarding(consumer sarama.Consumer, center_nodestru Nodestructure, wg *sync.WaitGroup) {
	partitonConsumer, err := consumer.ConsumePartition("KeyReqForwarding", 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	defer wg.Done()
	for {
		select {
		case msg := <-partitonConsumer.Messages():
			if err := KeyReqForwarding(center_nodestru, msg.Value); err != nil {
				log.Info("KeyReqForwarding error:", err)
			}
		case <-partitonConsumer.Errors():
			log.Critical("consumerkeyreqforwarding error")
		}
	}
}

func consumeFileReqestToCenter(consumer sarama.Consumer, center_nodestru Nodestructure, wg *sync.WaitGroup) {
	partitonConsumer, err := consumer.ConsumePartition("FileReqestToCenter", 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	defer wg.Done()
	for {
		select {
		case msg := <-partitonConsumer.Messages():
			if err := FileReqestToCenter(center_nodestru, msg.Value); err != nil {
				log.Info("FileReqestToCenter error:", err)
			}
		case <-partitonConsumer.Errors():
			log.Critical("consumeFileReqestToCenter error")
		}
	}
}

func consumeUploadKeyPosition(consumer sarama.Consumer, center_nodestru Nodestructure, wg *sync.WaitGroup) {
	partitonConsumer, err := consumer.ConsumePartition("UploadKeyPosition", 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	defer wg.Done()
	for {
		select {
		case msg := <-partitonConsumer.Messages():
			if err := UploadKeyPosition(center_nodestru, msg.Value); err != nil {
				log.Info("UploadKeyPosition error:", err)
			}
		case <-partitonConsumer.Errors():
			log.Critical("consumeUploadKeyPosition error")
		}
	}
}
