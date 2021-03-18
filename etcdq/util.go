/**
 * @Author: hiram
 * @Date: 2021/3/18 13:43
 */
package etcdq

import (
	"context"
	"encoding/json"
	"hash/crc32"
	"os"
	"strconv"
	"time"
)

func Stamp() int64 {
	return time.Now().UnixNano()
}
func HostName() string {
	ret, err := os.Hostname()
	if err != nil {
		Error.Println(err)
		return "defaultHost"
	}
	return ret
}
func Pid() string {
	return strconv.Itoa(os.Getpid())
}

func runWithMonitor(alive func() bool, asyncFunc ...func(stopCh chan struct{})) {
	// Monitor distributed lock for asyncFunc by alive func
	// If lock lose, release block,
	// cancel asyncFunc by close stopCh
	stopCh := make(chan struct{})
	for _, f := range asyncFunc {
		go f(stopCh)
	}
	for {
		if !alive() {
			close(stopCh)
			time.Sleep(ErrorSleep)
			return
		}
		time.Sleep(MonitorSleep)
	}
}

func StringToObject(str string, object interface{}) {
	err := json.Unmarshal([]byte(str), object)
	if err != nil {
		Error.Println("String convert to object fail: ", err)
	}
}

func ObjectToString(object interface{}) string {
	str, err := json.Marshal(object)
	if err != nil {
		Error.Println("Object convert to string Fail: ", err)
	}
	return string(str)
}

func Hash(s string) int {
	v := int(crc32.ChecksumIEEE([]byte(s)))
	if v >= 0 {
		return v
	}
	if -v >= 0 {
		return -v
	}
	return 0
}

func ctx() context.Context {
	ctx, _ := context.WithTimeout(context.TODO(), CtxTimeout)
	return ctx
}
