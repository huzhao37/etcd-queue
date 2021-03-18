/**
 * @Author: hiram
 * @Date: 2021/3/18 13:44
 */
package etcdq

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

type Watch interface {
	WatchPrefix(stopCh chan struct{})
	getRevision() int64
	setRevision(revision int64) KV
}

type Watcher struct {
	etcd        *Etcd
	key         string
	eventCh     chan Events
	queue       Queue
	resourceKey string
}

var _ Watch = new(Watcher)

func (w *Watcher) WatchPrefix(stopCh chan struct{}) {
	// Watch resource events, put into eventCh for customer usage
	go w.etcd.WatchPrefix(w.resourceKey, w.getRevision(), w.eventCh, stopCh)
	var (
		evsCache       KVs
		evsCacheLock   = new(sync.Mutex)
		latestRevision int64
	)

	// Write cached events to queue
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				if len(evsCache) != 0 {
					evsCacheLock.Lock()
					Info.Println("send queuesTasks", len(evsCache))
					evsCache = append(evsCache, w.setRevision(latestRevision))
					w.etcd.Atomic(evsCache, Ks{})
					evsCache = KVs{}
					evsCacheLock.Unlock()
				}
				time.Sleep(WatchSleep)
			}
		}
	}()

	// Read events from eventCh, write into cache, if eventCh closed, stop read
	for {
		select {
		case newEvs, ok := <-w.eventCh:
			if !ok {
				Info.Println("Watch stopped")
				return
			}
			evsCacheLock.Lock()
			for _, ev := range newEvs {
				parts := strings.Split(ev.key, "/")
				item := parts[len(parts)-1]
				queueValue := QueueValue{EventType: ev.eventType, Value: ev.value,}
				stamp := strconv.Itoa(int(ev.stamp))
				evsCache = append(evsCache, w.queue.enqueue(item, stamp, queueValue))
				// if evsCache reach MaxTxnOps, write events into queue
				if len(evsCache) == w.etcd.MaxTxnOps-1 {
					Info.Println("Watch event into queue", len(evsCache))
					evsCache = append(evsCache, w.setRevision(ev.revision))
					w.etcd.Atomic(evsCache, Ks{})
					evsCache = KVs{}
				}
			}
			latestRevision = newEvs[len(newEvs)-1].revision
			evsCacheLock.Unlock()
		}
	}
}

func (w *Watcher) getRevision() int64 {
	kv := w.etcd.ReadItem(w.key)
	if kv == emptyKV {
		return 1
	}
	prevRevision, err := strconv.Atoi(kv.value)
	if err != nil {
		Error.Println(err)
	}
	return int64(prevRevision + 1)
}
func (w *Watcher) setRevision(revision int64) KV {
	return KV{
		key:   w.key,
		value: strconv.Itoa(int(revision)),
	}
}

func NewWatcher(etcd *Etcd, queue Queue, resource string) Watcher {
	return Watcher{
		etcd:        etcd,
		eventCh:     make(chan Events),
		queue:       queue,
		resourceKey: ResourcePrefix + resource,
		key:         WatcherPrefix + resource,
	}
}
