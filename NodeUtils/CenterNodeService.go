package NodeUtils

import (
	"encoding/json"
	"fabric-go-sdk/clients"
	"fmt"

	"github.com/fatih/structs"
)

// if exist update, if not exist upload ,
func uploadFilePosition(center_nodestru Nodestructure, msg []byte) (err error) {
	var file_position_info PositionInfo
	err = json.Unmarshal(msg, &file_position_info)
	if err != nil {
		return fmt.Errorf("unmarshal error: %v", err)
	}
	cc, err := clients.GetCouchdb(center_nodestru.Couchdb_addr)
	if err != nil {
		return fmt.Errorf("get couchdb error: %v", err)
	}
	if NotExist, err := cc.CheckNotExistence(file_position_info.FileId, "position_info"); err != nil {
		return fmt.Errorf("check not exist error: %v", err)
	} else if NotExist {
		//not exist,upload
		if err = cc.CouchdbCreate(file_position_info.FileId, structs.Map(&file_position_info), "position_info"); err != nil {
			return fmt.Errorf("upload error: %v", err)
		}
	} else {
		if err = cc.CouchdbUpdate(file_position_info.FileId, "Position", file_position_info.Position, "position_info"); err != nil {
			return fmt.Errorf("update error: %v", err)
		}
	}
	return nil
}

func FileReqestToCenter(center_nodestru Nodestructure, msg []byte) (err error) {
	//receive request
	var filereqstru FileRequest
	err = json.Unmarshal(msg, &filereqstru)
	if err != nil {
		return fmt.Errorf("unmarshal error: %v", err)
	}
	cc, err := clients.GetCouchdb(center_nodestru.Couchdb_addr)
	if err != nil {
		return fmt.Errorf("get couchdb error: %v", err)
	}
	//check if file exist and if exist send to node linking client
	//if not exist check position of file
	//else return error
	if NotExist, err1 := cc.CheckNotExistence(filereqstru.FileId, "ciphertext_info"); err != nil {
		return fmt.Errorf("check not exist error: %v", err1)
	} else if NotExist {
		//file not exist,check positon of file
		if positionNotExist, err2 := cc.CheckNotExistence(filereqstru.FileId, "position_info"); err2 != nil {
			return fmt.Errorf("check not exist error: %v", err2)
		} else if positionNotExist {
			return fmt.Errorf("file not exist")
		} else {
			//send request to the node which has the file
			rs, err2 := cc.Getinfo(filereqstru.FileId, "position_info")
			if err2 != nil {
				return fmt.Errorf("db.get error: %v", err2)
			}
			var posinfo FilePositionInfoDTO
			err2 = rs.ScanDoc(&posinfo)
			if err2 != nil {
				fmt.Println("scandoc error: ", err2)
				return
			}
			err2 = ProducerAsyncSending(msg, "ReceiveFileRequestFromCenter", posinfo.Position)
			if err2 != nil {
				fmt.Println("receive request producer sending error ", err2)
				return
			}
		}
	} else {
		//file exist,send to node linking client
		var fileif FileInfo
		rs, err1 := cc.Getinfo(filereqstru.FileId, "ciphertext_info")
		if err1 != nil {
			return fmt.Errorf("db.get error: %v", err1)
		}
		err1 = rs.ScanDoc(&fileif)
		if err != nil {
			return fmt.Errorf("scandoc error: %v", err1)
		}
		data := fileif.Ciphertext
		//forward to the node linking to client
		res, err1 := json.Marshal(DataSend2clientInfo{
			UserId: filereqstru.UserId,
			Data:   []byte(data),
			FileId: filereqstru.FileId,
		})
		if err1 != nil {
			return fmt.Errorf("fail to Serialization, err:%v", err1)
		}
		if err1 = ProducerAsyncSending(res, "DataForwarding", filereqstru.Kafka_addr); err != nil {
			return fmt.Errorf("producer async sending err: %v", err1)
		}
	}
	return nil
}

func UploadCiText(center_nodestru Nodestructure, msg []byte) (err error) {
	//receive ciphertext
	fmt.Println("<------upload citext---->")
	var fileinfostru FileInfo
	err = json.Unmarshal(msg, &fileinfostru)
	if err != nil {
		return fmt.Errorf("unmarshal error: %v", err)
	}
	cc, err := clients.GetCouchdb(center_nodestru.Couchdb_addr)
	if err != nil {
		return fmt.Errorf("get couchdb error: %v", err)
	}
	err = cc.CouchdbCreate(fileinfostru.FileId, structs.Map(&fileinfostru), "ciphertext_info")
	if err != nil {
		return fmt.Errorf("upload error: %v", err)
	}
	//change the position info of ciphertext
	//get rev first

	//modify the position in center
	if err = cc.CouchdbUpdate(fileinfostru.FileId, "Position", center_nodestru.KafkaAddr, "position_info"); err != nil {
		return fmt.Errorf("update error: %v", err)
	}
	return nil
}

func UploadKeyPosition(center_nodestru Nodestructure, msg []byte) (err error) {
	//receive key position
	fmt.Println("<------upload keyposition---->")
	var key_positon KeyPostionUploadInfo
	err = json.Unmarshal(msg, &key_positon)
	if err != nil {
		return fmt.Errorf("unmarshal error: %v", err)
	}

	cc, err := clients.GetCouchdb(center_nodestru.Couchdb_addr)
	if err != nil {
		return fmt.Errorf("get couchdb error: %v", err)
	}

	err = cc.CouchdbUpdate(key_positon.FileId, "GroupAddrs", key_positon.GroupAddrs, "position_info")
	if err != nil {
		fmt.Println(err)
	}
	return

}

// 接收密钥请求，根据元数据把请求转发到各个节点
// TODO:负载均衡
func KeyReqForwarding(center_nodestru Nodestructure, msg []byte) (err error) {
	//receive request
	var freq FileRequest
	err = json.Unmarshal(msg, &freq)
	if err != nil {
		return fmt.Errorf("unmarshal error: %v", err)
	}
	cc, err := clients.GetCouchdb(center_nodestru.Couchdb_addr)
	if err != nil {
		return fmt.Errorf("get couchdb error: %v", err)
	}
	resultSet, err := cc.Getinfo(freq.FileId, "position_info")
	if err != nil {
		return fmt.Errorf("db.get error: %v", err)
	}
	var position_info PositionInfo
	err = resultSet.ScanDoc(&position_info)
	if err != nil {
		return fmt.Errorf("scandoc error: %v", err)
	}
	choosed_addr, err := loadbalance(position_info.GroupAddrs, 3)
	if err != nil {
		return fmt.Errorf("loadbalance error: %v", err)
	}
	for _, kafka_addr := range choosed_addr {
		topic := "ReceiveKeyReq" //操作名
		err = ProducerAsyncSending(msg, topic, kafka_addr)
		if err != nil {
			fmt.Println("producer async sending err:", err)
			break
		}
	}
	return
}
