/**
 * @Author: hiram
 * @Date: 2021/3/18 13:40
 */

package etcdq

import "time"

const (
	ResourcePrefix = "/resources/" // custom resource operation, generate events
	WatcherPrefix  = "/watchers/"  // global single watcher, watch events
	QueuePrefix    = "/queues/"    // central queue, storage events
	HandlerPrefix  = "/handlers/"  // handler's own events to handle
	LockPrefix     = "/locks"      // handler's registered lock
)

const (
	WatchSleep    = time.Millisecond * 100
	ScheduleSleep = time.Millisecond * 100
	HandlerSleep  = time.Millisecond * 100
	MonitorSleep  = time.Second * 2
	ErrorSleep    = time.Second * 5
	CtxTimeout    = time.Second * 10
)

type Event_EventType int32

const (
	PUT    Event_EventType = 0
	DELETE Event_EventType = 1
)

//  ./etcd.exe --max-txn-ops="32768" --max-request-bytes="536870912"
const (
	MaxCallSendMsgSize = 512 * 1024 * 1024
	MaxCallRecvMsgSize = 512 * 1024 * 1024
	MaxTxnOps          = 32768
)
