package NodeUtils

import (
	httpapi "fabric-go-sdk/http_api"
	"math/rand"
	"sort"
	"time"
)

// // 生成正态分布随机数
// func randomDuration() time.Duration {
// 	rand.Seed(time.Now().UnixNano())

// 	// 正态分布随机数
// 	duration := 0.1*rand.NormFloat64() + 0.8

// 	// 保证在0-1范围内
// 	if duration < 0 {
// 		duration = 0.01
// 	}
// 	if duration > 1 {
// 		duration = 1
// 	}

// 	return time.Duration(duration) * time.Second
// }

// func (n *NodeInfo) handleTask() {
// 	for {
// 		//如果任务数为0，则阻塞
// 		<-n.taskChan
// 		//处理任务
// 		time.Sleep(randomDuration())
// 		totalTasks++
// 	}
// }

// type Group []string

type loadInfo struct {
	addr string
	load float64
}

func loadbalance(g []string, k int) (gro []string, err error) {
	group_load := make([]loadInfo, len(g))
	for i, addr := range g {
		//询问每个节点的cpu占用率
		load, err := httpapi.GetLoadFromGroupNode(addr)
		if err != nil {
			return nil, err
		}
		group_load[i].addr = addr
		group_load[i].load = load
	}
	//对group_load进行排序
	sort.Slice(group_load, func(i, j int) bool {
		return group_load[i].load < group_load[j].load
	})
	//把任务数最少的k个节点加入gro
	for i := 0; i < k; i++ {
		gro = append(gro, group_load[i].addr)
	}
	return
}

func NotLoadbalance(g []string, k int) (gro []string, err error) {
	//返回随机的k个节点
	group_len := len(g)
	// 初始化随机数生成器
	rand.Seed(time.Now().UnixNano())

	// 生成n个不重复的随机数
	nums := make([]int, group_len)
	for i := 0; i < group_len; i++ {
		nums[i] = i
	}
	rand.Shuffle(group_len, func(i, j int) {
		nums[i], nums[j] = nums[j], nums[i]
	})

	// 取前k个随机数
	if k > group_len {
		k = group_len
	}
	for i := 0; i < k; i++ {
		gro = append(gro, (g)[nums[i]])
	}
	return
}
