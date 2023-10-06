package main

import (
	"fabric-go-sdk/NodeUtils"
	"os"
)

var center = NodeUtils.Nodestructure{
	KafkaAddr:    os.Getenv("KAFKA_ADDR"),
	Couchdb_addr: os.Getenv("COUCHDB_ADDR"),
}

// var peertopics = []string{"register", "upload", "filereq", "KeyUpload", "ReceiveKeyUpload", "ReceiveKeyReq", "DataForwarding", "ReceiveFileRequestFromCenter"}
var centertopics = []string{"uploadposition", "UploadCiText", "KeyReqForwarding", "FileReqestToCenter", "UploadKeyPosition"}

func main() {

	NodeUtils.InitCenterNode(centertopics, center)

}
