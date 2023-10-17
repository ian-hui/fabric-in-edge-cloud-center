package clients

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-kivik/kivik/v4"
)

type CouchdbClient struct {
	C  *kivik.Client
	Mu sync.Mutex
}

var (
	couchdbConns *CouchdbClient
)

// func InitCouchdb(kivik_addr string) error {
// 	client, err := kivik.New("couch", kivik_addr)
// 	if err != nil {
// 		return fmt.Errorf("init couchdb client error: %v", err)
// 	}
// 	kivikclient := new(couchdbClient)
// 	kivikclient.C = client
// 	couchdbConns.Store(kivik_addr, kivikclient)
// 	return nil
// }

func GetCouchdb(kivik_addr string) (*CouchdbClient, error) {
	if couchdbConns == nil {
		mu.Lock()
		defer mu.Unlock()
		if couchdbConns == nil {
			fmt.Println("init couchdb client" + kivik_addr)
			client, err := kivik.New("couch", kivik_addr)
			if err != nil {
				return nil, err
			}
			kivikclient := new(CouchdbClient)
			kivikclient.C = client
			kivikclient.Mu = sync.Mutex{}
			couchdbConns = kivikclient
		}
	}
	return couchdbConns, nil
}

func (client *CouchdbClient) Create_ciphertext_info() error { //create db in couchdb
	err := client.C.CreateDB(context.TODO(), "ciphertext_info", nil)
	if err != nil {
		return fmt.Errorf("create ciphertext_info db error: %v", err)
	}
	fmt.Println("<-------", client.C, " couchdb ciphertext_info created!------>")
	return nil
}

func (client *CouchdbClient) Create_position_info() error { //create db in couchdb
	err := client.C.CreateDB(context.TODO(), "position_info", nil)
	if err != nil {
		return fmt.Errorf("create position_info db error: %v", err)
	}
	fmt.Println("<-------", client.C, " couchdb position_info created!------>")
	return nil
}

// func (client *CouchdbClient) UploadPostion(id string, m map[string]interface{}) error {

// 	db := client.C.DB("position_info", nil) //连接couchdb中的positon_info数据库
// 	_, err := db.Put(context.TODO(), id, m) //把数据info上传到db
// 	if err != nil {
// 		panic(err)
// 	}
// 	return nil
// }

func (client *CouchdbClient) CouchdbCreate(id string, m map[string]interface{}, dbname string) error {
	// json_fileposif := structs.Map(&f) //转格式，详细看https://github.com/go-kivik/kivik
	db := client.C.DB(dbname, nil)            //连接couchdb中的cipher_info数据库
	rev, err := db.Put(context.TODO(), id, m) //把数据info上传到db
	if err != nil {
		client.CouchdbCreate(id, m, dbname)
	}
	fmt.Printf("%s inserted with revision %s\n", id, rev)
	return nil
}

func (client *CouchdbClient) Getinfo(id string, dbname string) (kivik.ResultSet, error) {
	db := client.C.DB(dbname, nil) //connect to position_info
	resultSet := db.Get(context.TODO(), id)
	if resultSet.Err() != nil {
		return nil, resultSet.Err()
	}
	return resultSet, nil

}

func (client *CouchdbClient) CheckNotExistence(id string, dbname string) (bool, error) {
	_, err := client.Getinfo(id, dbname)
	if err != nil {
		if kivik.StatusCode(err) == 404 {
			//确实不存在
			return true, nil
		}
		return false, fmt.Errorf("db.get error: %v", err)
	}
	return false, nil
}

// func UploadCipherKey(f KeyDetailInfo, nodestru Nodestructure) error {
// 	json_fileposif := structs.Map(&f) //转为map[string]interface{}格式
// 	client, err := clients.GetCouchdb(nodestru.Couchdb_addr)
// 	if err != nil {
// 		return fmt.Errorf("get couchdb client error: %v", err)
// 	}
// 	client.Mu.Lock()
// 	defer client.Mu.Unlock()
// 	db := client.C.DB("cipherkey_info", nil)                  //connect to ciphertext_info
// 	_, err = db.Put(context.TODO(), f.FileId, json_fileposif) //把数据info上传到db
// 	if err != nil {
// 		return fmt.Errorf("upload cipherkey error: %v", err)
// 	}
// 	// fmt.Printf("%s inserted with revision %s\n", f.FileId, rev)
// 	return nil
// }

//把targetdata的值改为changedata
func (client *CouchdbClient) CouchdbUpdate(id string, targetdata string, changedata interface{}, dbname string) error {
	// client.Mu.Lock()
	// defer client.Mu.Unlock()
	db := client.C.DB(dbname, nil) //connect to position_info
	resultSet := db.Get(context.TODO(), id)

	var doc map[string]interface{}
	err := resultSet.ScanDoc(&doc)
	if err != nil {
		return fmt.Errorf("scandoc error: %v", err)
	}

	//增加通用性
	switch v := changedata.(type) {
	case string:
		doc[targetdata] = v
	case []string:
		_, ok := doc[targetdata]
		if !ok {
			return fmt.Errorf("field '%s' not found in doc", targetdata)
		}
		doc[targetdata] = v
	case [][]string:
		_, ok := doc[targetdata]
		if !ok {
			return fmt.Errorf("field '%s' not found in doc", targetdata)
		}
		doc[targetdata] = v
	default:
		return fmt.Errorf("unsupported data type: %T", v)
	}

	_, err = db.Put(context.TODO(), id, doc) //把数据info上传到db
	if err != nil {
		//乐观锁，重新执行任务
		client.CouchdbUpdate(id, targetdata, changedata, dbname)
	}
	return nil
}
