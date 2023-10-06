package httpapi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

func GetLoadFromGroupNode(httpaddr string) (load float64, err error) {
	resp, err := http.Get("http://" + httpaddr + "/load")
	fmt.Println("http://" + httpaddr + "/load")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	//获取resp的body的load
	if resp.StatusCode == http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return 0, fmt.Errorf("read resp body error: %v", err)
		}
		var data map[string]interface{}
		err = json.Unmarshal(body, &data)
		if err != nil {
			return 0, fmt.Errorf("unmarshal error: %v", err)
		}
		// 提取load的值
		load := data["load"].(float64)
		fmt.Println(load)
		return load, nil
	} else {
		return 0, fmt.Errorf("get load error: %v", err)
	}
}
