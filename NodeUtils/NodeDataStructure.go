package NodeUtils

type Nodestructure struct {
	KafkaAddr    string
	Couchdb_addr string
}

type PositionInfo struct {
	// AreaId   string `json:"AreaId"`
	FileId        string   `json:"FileId"`
	FilePosition  string   `json:"FilePosition"`
	KeyGroupAddrs []string `json:"KeyGroupAddrs"`
}

type FileInfo struct {
	FileId     string `json:"FileId"`
	Ciphertext string `json:"Ciphertext"`
}

type FileRequest struct {
	FileId     string `json:"FileId"`
	UserId     string `json:"UserId"`
	Kafka_addr string `json:"Kafka_addr"`
}

type FileRequestDTO struct {
	FileId     string `json:"FileId"`
	Ciphertext string `json:"Ciphertext"`
	Id         string `json:"_id"`
	Rev        string `json:"_rev"`
}
type FilePositionInfoDTO struct {
	FileId   string `json:"FileId"`
	Position string `json:"Position"`
	AreaId   string `json:"AreaId"`
	Rev      string `json:"_rev"`
	Id       string `json:"_id"`
}

type KeyPostionUploadInfo struct {
	FileId     string   `json:"FileId"`
	GroupAddrs []string `json:"GroupAddrs"`
}

type DataSend2clientInfo struct {
	TransferFlag bool   `json:"TransferFlag"`
	Data         []byte `json:"Data"`
	FileId       string `json:"FileId"`
	UserId       string `json:"UserId"`
}
