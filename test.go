/**
 * @Author: hiram
 * @Date: 2021/3/18 13:43
 */
package main

import (
	"gitee.com/gbat/etcd-queue/etcdq"
	"strconv"
	"sync"
	"time"
)

func writeItem(etcd *etcdq.Etcd, key string, value string, wg *sync.WaitGroup) {
	defer wg.Done()
	etcd.WriteItem(key, value)
}
func writeEvents(n int) {
	etcd := etcdq.NewEtcd("127.0.0.1:2379", 10)
	var wg sync.WaitGroup
	num := 2000
	for i := 0; i < num; i++ {
		key := etcdq.ResourcePrefix + "apple/" + strconv.Itoa(i) + "#" + strconv.Itoa(n) + "%"
		wg.Add(1)
		go writeItem(&etcd, key, key, &wg)
	}
	wg.Wait()
	etcdq.Info.Println("write events: ", n*num)
}
func main() {
	for i := 0; i < 50; i++ {
		writeEvents(i)
		time.Sleep(time.Second*1)
	}
}

