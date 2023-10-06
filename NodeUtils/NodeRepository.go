package NodeUtils

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fabric-go-sdk/clients"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/IBM/sarama"
	_ "github.com/go-kivik/couchdb/v4" // The CouchDB driver
	//couchdb-go第三方库
)

func GetAreaKafkaAddrInZookeeper(area_num string) []string {
	var res map[string]interface{}
	kafka_addr := make([]string, 0)
	conn := clients.ZookeeperConns
	children, _, err := conn.Children("/peer/area" + area_num)
	if err != nil {
		panic(err)
	}

	for _, child := range children {
		endfilenames, _, err := conn.Children("/peer/area" + area_num + "/" + child + "/brokers/ids")
		if err != nil {
			panic(err)
		}
		for _, fname := range endfilenames {
			data, _, err := conn.Get("/peer/area" + area_num + "/" + child + "/brokers/ids/" + fname)
			if err != nil {
				panic(err)
			}
			json.Unmarshal(data, &res)
			temp := res["port"].(float64)
			port := strconv.FormatFloat(temp, 'f', 0, 64)
			kafka_addr = append(kafka_addr, "0.0.0.0:"+port)
			// fmt.Printf("Kafka node %s is running at %s\n", child, data)
		}

	}
	return kafka_addr
}

func GetAllPeerAddrInZookeeper() []string {
	var res map[string]interface{}
	kafka_addr := make([]string, 0)
	conn := clients.ZookeeperConns
	areas, _, err := conn.Children("/peer")
	if err != nil {
		panic(err)
	}
	for _, area := range areas {
		endpointnames, _, err := conn.Children("/peer/" + area)
		if err != nil {
			panic(err)
		}
		for _, endpointname := range endpointnames {
			endfilenames, _, err := conn.Children("/peer/" + area + "/" + endpointname + "/brokers/ids")
			if err != nil {
				panic(err)
			}
			for _, fname := range endfilenames {
				data, _, err := conn.Get("/peer/" + area + "/" + endpointname + "/brokers/ids/" + fname)
				if err != nil {
					panic(err)
				}
				json.Unmarshal(data, &res)
				temp := res["port"].(float64)
				port := strconv.FormatFloat(temp, 'f', 0, 64)
				kafka_addr = append(kafka_addr, "0.0.0.0:"+port)
				// fmt.Printf("Kafka node %s is running at %s\n", child, data)
			}
		}

	}
	return kafka_addr
}

func GetCenterKafkaAddrInZookeeper() []string {
	var res map[string]interface{}
	center_kafka_addr := make([]string, 0, 4)
	conn := clients.ZookeeperConns
	children, _, err := conn.Children("/center/brokers/ids")
	if err != nil {
		panic(err)
	}

	for _, child := range children {
		data, _, err := conn.Get("/center/brokers/ids/" + child)
		if err != nil {
			panic(err)
		}
		json.Unmarshal(data, &res)
		temp := res["port"].(float64)
		port := strconv.FormatFloat(temp, 'f', 0, 64)
		center_kafka_addr = append(center_kafka_addr, "0.0.0.0:"+port)
	}
	return center_kafka_addr
}

func ProducerAsyncSending(messages []byte, topic string, kafka_addr string) error {
	kafka_client, err := clients.GetProducer(kafka_addr)
	if err != nil {
		return err
	}
	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	err = AsyncSend2Kafka(kafka_client, msg, messages)
	if err != nil {
		return err
	}
	return nil
}

// 异步发函数函数
func AsyncSend2Kafka(client sarama.AsyncProducer, msg *sarama.ProducerMessage, content []byte) error {
	msg.Value = sarama.StringEncoder(content)

	// 发送消息
	client.Input() <- msg
	go func() {
		select {
		case success := <-client.Successes():
			fmt.Println("message sent successfully")
			fmt.Printf("partition:%v offset:%v\n", success.Partition, success.Offset)
		case err := <-client.Errors():
			panic(err)
		}
	}()
	return nil
}

func DeleteTargetInArrayStr(array_str []string, target string) []string {
	j := 0
	for _, val := range array_str {
		if val != target {
			array_str[j] = val
			j++
		}
	}
	return array_str[:j]
}

func IsIdentical(str1 []string, str2 []string) (t bool) {
	t = false
	if len(str1) == 0 || len(str2) == 0 {
		return
	}
	map1, map2 := make(map[string]int), make(map[string]int)
	for i := 0; i < len(str1); i++ {
		map1[str1[i]] = i
	}
	for i := 0; i < len(str2); i++ {
		map2[str2[i]] = i
	}
	for k := range map1 {
		if _, ok := map2[k]; ok {
			t = true
		}
	}
	return
}

func NewTLSConfig() (*tls.Config, error) {
	// Load client cert
	cert, err := tls.LoadX509KeyPair("./kafka_crypto/client.cer.pem", "./kafka_crypto/client.key.pem")
	if err != nil {
		return nil, err
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile("./kafka_crypto/server.cer.pem")
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
	}
	return tlsConfig, err
}
