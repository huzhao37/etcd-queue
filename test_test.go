/**
 * @Author: hiram
 * @Date: 2021/3/18 14:06
 */
package main

import (
	"gitee.com/gbat/etcd-queue/etcdq"
	"strconv"
	"testing"
)

func Benchmark_writeEvents(b *testing.B) {
	etcd := etcdq.NewEtcd("127.0.0.1:2379", 10)
    b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := etcdq.ResourcePrefix + "apple/" + strconv.Itoa(i) + "#" + "haha" + "%"
		etcd.WriteItem(key, "haha")
	}

}