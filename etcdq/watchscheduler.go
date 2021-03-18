/**
 * @Author: hiram
 * @Date: 2021/3/18 13:44
 */
package etcdq

import (
	"time"
)

type watchSchedule interface {
	schedule(stopCh chan struct{})
	watch(stopCh chan struct{})
}

type WatchScheduleGroup struct {
	resource          string
	ttl               int64
	etcd              *Etcd
	queue             Queue
	handlerPrefix     string
	handlerLockPrefix string
}

var _ watchSchedule = new(WatchScheduleGroup)

func (wsg WatchScheduleGroup) schedule(stopCh chan struct{}) {
	Info.Println("scheduler started")
	scheduler := NewScheduler(wsg.etcd)
	// Schedule events, if lock lose, stop schedule
	scheduler.InitPrevAliveHandlers(wsg.handlerPrefix)
	for {
		select {
		case <-stopCh:
			Info.Println("scheduler stopped")
			return
		default:
			scheduler.Distribute(wsg.handlerPrefix, wsg.queue)
			scheduler.Redistribute(wsg.handlerLockPrefix, wsg.queue)
			time.Sleep(ScheduleSleep)
		}
	}
}

func (wsg WatchScheduleGroup) watch(stopCh chan struct{}) {
	Info.Println("watcher started")
	watcher := NewWatcher(wsg.etcd, wsg.queue, wsg.resource)
	watcher.WatchPrefix(stopCh)
}

func (wsg WatchScheduleGroup) Run() {
	for {
		aliveFunc := wsg.etcd.SpinLock(WatcherPrefix+wsg.resource, wsg.ttl)
		if aliveFunc != nil {
			runWithMonitor(aliveFunc, wsg.watch, wsg.schedule)
		}
		time.Sleep(ErrorSleep)
	}
}

func NewWatchScheduleGroup(resource string, etcd *Etcd) WatchScheduleGroup {
	return WatchScheduleGroup{
		resource:          resource,
		ttl:               10,
		handlerPrefix:     HandlerPrefix + resource,
		handlerLockPrefix: LockPrefix + HandlerPrefix + resource,
		etcd:              etcd,
		queue:             NewQueue(etcd, resource),
	}
}

func RunWatch() {
	etcd := NewEtcd("127.0.0.1:2379", 20)
	defer etcd.client.Close()
	wsg := NewWatchScheduleGroup("apple", &etcd)
	wsg.Run()
}

