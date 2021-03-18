/**
 * @Author: hiram
 * @Date: 2021/3/18 13:42
 */
package etcdq

import (
	"strings"
)

type Schedule interface {
	InitPrevAliveHandlers(handlerPrefix string)
	Distribute(handlerPrefix string, queue Queue)
	Redistribute(handlerLockPrefix string, queue Queue)
}

type Scheduler struct {
	etcd          *Etcd
	aliveHandlers []string
}

var _ Schedule = new(Scheduler)

func (s *Scheduler) getAliveHandlers(handlerLockPrefix string) []string {
	var aliveHandlers []string
	for _, kv := range s.etcd.ReadItems(handlerLockPrefix) {
		aliveHandlers = append(aliveHandlers, kv.key[len(LockPrefix):])
	}
	return aliveHandlers
}
func (s *Scheduler) getHandlerBindingItems(handlerPrefix string) map[string]Set {
	handlerBindingItems := make(map[string]Set)
	for _, kv := range s.etcd.ReadItems(handlerPrefix) {
		key := kv.key
		handler := key[:strings.LastIndex(key, "/")]
		task := strings.Trim(key, handler)
		item := task[:strings.LastIndex(task, "-")]
		items, ok := handlerBindingItems[handler]
		if !ok {
			handlerBindingItems[handler] = NewSet()
			items = handlerBindingItems[handler]
		}
		items.Add(item)
	}
	return handlerBindingItems
}
func (s *Scheduler) InitPrevAliveHandlers(handlerPrefix string) {
	var prevHandlers []string
	for _, kv := range s.etcd.ReadItems(handlerPrefix) {
		key := kv.key
		prevHander := key[:strings.LastIndex(key, "/")]
		prevHandlers = append(prevHandlers, prevHander)
	}
	s.aliveHandlers = prevHandlers
}
func (s *Scheduler) Distribute(handlerPrefix string, queue Queue) {
	handlerNum := len(s.aliveHandlers)
	if handlerNum == 0 {
		return
	}
	queuesTasks := queue.dequeue()
	if len(queuesTasks) == 0 {
		return
	}
	//Debug.Println("recv queuesTasks", len(queuesTasks))
	handlerBindingItems := s.getHandlerBindingItems(handlerPrefix)
	var deletes Ks
	var writes KVs
	for _, kv := range queuesTasks {
		key := kv.key
		value := kv.value
		task := strings.Split(key[strings.LastIndex(key, "/")+1:], "-")
		item := task[0]
		stamp := task[1]
		allocIndex := Hash(item) % handlerNum
		// Allocate handler for item
		handlerKey := s.aliveHandlers[allocIndex]
		// New handler member, haven't bind items
		if _, ok := handlerBindingItems[handlerKey]; !ok {
			for bindingKey, bindingItems := range handlerBindingItems {
				// Item have been bind to old handler
				if bindingItems.Contains(item) {
					handlerKey = bindingKey
					break
				}
			}
		}
		eventKey := handlerKey + "/" + item + "-" + stamp
		deletes = append(deletes, key)
		writes = append(writes, KV{
			key:   eventKey,
			value: value,
		})
	}
	p := s.etcd.MaxTxnOps/2
	for i := 0; i < len(writes); i += p {
		if i+p > len(writes) {
			s.etcd.Atomic(writes[i:], deletes[i:])
		} else {
			s.etcd.Atomic(writes[i:i+p], deletes[i:i+p])
		}
	}
}
func (s *Scheduler) Redistribute(handlerLockPrefix string, queue Queue) {
	aliveHandlers := s.getAliveHandlers(handlerLockPrefix)
	if len(aliveHandlers) == 0 {
		return
	}
	var deletes Ks
	var writes KVs
	for _, oldHandler := range s.aliveHandlers {
		thisOldHandlerAlive := false
		for _, aliveHandler := range aliveHandlers {
			if oldHandler == aliveHandler {
				thisOldHandlerAlive = true
				break
			}
		}
		if thisOldHandlerAlive {
			continue
		}
		oldHandlerTasks := s.etcd.ReadItems(oldHandler)
		for _, kv := range oldHandlerTasks {
			key := kv.key
			value := kv.value
			task := key[strings.LastIndex(key, "/")+1:]
			rets := strings.Split(task, "-")
			item := rets[0]
			stamp := rets[1]
			queueValue := QueueValue{}
			StringToObject(value, &queueValue)
			deletes = append(deletes, key)
			writes = append(writes, queue.enqueue(item, stamp, queueValue))
		}
	}
	if len(writes) != 0 {
		p := s.etcd.MaxTxnOps
		for i := 0; i < len(writes); i += p {
			if i+p > len(writes) {
				s.etcd.Atomic(writes[i:], deletes[i:])
			} else {
				s.etcd.Atomic(writes[i:i+p], deletes[i:i+p])
			}
		}
	}
	s.aliveHandlers = aliveHandlers
}
func NewScheduler(etcd *Etcd) Scheduler {
	return Scheduler{
		etcd: etcd,
	}
}
