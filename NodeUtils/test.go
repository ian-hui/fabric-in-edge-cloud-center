package NodeUtils

type Test_data struct {
	Keypath     []string
	Ip          []string
	Username    string
	Userkeypath string
	Fileid      string
	Attribute   []string
}

var Test_data1 = Test_data{
	Keypath: []string{"/home/go/src/fabric-go-sdk/fixtures/crypto-config/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/msp/keystore/priv_sk",
		"/home/go/src/fabric-go-sdk/fixtures/crypto-config/peerOrganizations/org1.example.com/peers/peer1.org1.example.com/msp/keystore/priv_sk",
		"/home/go/src/fabric-go-sdk/fixtures/crypto-config/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/msp/keystore/priv_sk",
		"/home/go/src/fabric-go-sdk/fixtures/crypto-config/peerOrganizations/org2.example.com/peers/peer1.org2.example.com/msp/keystore/priv_sk"},
	Ip:          []string{"0.0.0.0:9092", "0.0.0.0:9093", "0.0.0.0:9094", "0.0.0.0:9095"},
	Username:    "ianhui",
	Userkeypath: "/home/go/src/fabric_in_edge-cloud/kafka_crypto/ianhui.private.pem",
	Fileid:      "1",
	Attribute:   []string{"老师"},
}
