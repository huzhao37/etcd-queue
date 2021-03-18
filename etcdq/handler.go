/**
 * @Author: hiram
 * @Date: 2021/3/18 13:41
 */
package etcdq

import (
	"strings"
	"sync"
	"time"
)

type SyncHandle interface {
	Receive() KVs                                       // receive events distributed from queue
	GroupBy(*KVs)                                       // group tasks by prefix
	Merge(taskPrefix string)                            // merge events of one item
	handleEvents(taskPrefix string, wg *sync.WaitGroup) // handle events of one item
	handleEvent(task KV)                                // handle one event of item
	Execute()                                           // execute tasks with WaitGroup
}

type Handler struct {
	etcd       *Etcd
	key        string
	workers    int
	ttl        int64
	done       Ks
	doneLock   *sync.Mutex
	resource   string
	taskGroup  map[string]KVs
	handleFunc func(task KV) bool
	mergeFunc  func(taskPrefix string)
	num int
}

var _ SyncHandle = new(Handler)

func (h *Handler) Receive() KVs {
	return h.etcd.ReadItems(h.key)
}

func (h *Handler) GroupBy(tasks *KVs) {
	h.taskGroup = make(map[string]KVs)
	for _, task := range *tasks {
		prefix := strings.Split(task.key, "-")[0]
		tasks, ok := h.taskGroup[prefix]
		if !ok {
			h.taskGroup[prefix] = KVs{task}
			continue
		}
		tasks = append(tasks, task)
	}
}

func (h *Handler) Execute() {
	var wg sync.WaitGroup
	for prefix, _ := range h.taskGroup {
		wg.Add(1)
		go h.handleEvents(prefix, &wg)
	}
	wg.Wait()
	p := h.etcd.MaxTxnOps
	for i := 0; i < len(h.done); i += p {
		if i+p > len(h.done) {
			h.etcd.Atomic(KVs{}, h.done[i:])
		} else {
			h.etcd.Atomic(KVs{}, h.done[i:i+p])
		}
	}

	if len(h.taskGroup) != 0 {
		h.num += len(h.taskGroup)
		Info.Println("done num: ", h.num)
	}
}

func (h *Handler) handleEvents(prefix string, wg *sync.WaitGroup) {
	defer wg.Done()
	if h.mergeFunc == nil {
		h.Merge(prefix)
	} else {
		h.mergeFunc(prefix)
	}
	for _, task := range h.taskGroup[prefix] {
		h.handleEvent(task)
	}
}

func (h *Handler) handleEvent(task KV) {
	if ok := h.handleFunc(task); ok {
		h.doneLock.Lock()
		defer h.doneLock.Unlock()
		h.done = append(h.done, task.key)
	}
}

func (h *Handler) Merge(prefix string) {
	tasks := h.taskGroup[prefix]
	for index, task := range tasks {
		queueValue := QueueValue{}
		StringToObject(task.value, &queueValue)
		eventType := queueValue.EventType
		if eventType == DELETE {
			var deletes Ks
			for _, vk := range tasks[index+1:] {
				deletes = append(deletes, vk.key)
			}
			p := h.etcd.MaxTxnOps
			for i := 0; i < len(deletes); i += p {
				if i+p > len(deletes) {
					h.etcd.Atomic(KVs{}, deletes[i:])
				} else {
					h.etcd.Atomic(KVs{}, deletes[i:i+p])
				}
			}

			h.taskGroup[prefix] = KVs{task}
			return
		}
	}
	// TODO: sort events by stamp
	//h.taskGroup[prefix] = h.taskGroup[prefix]
}

func (h *Handler) syncHandleEvents(stopCh chan struct{}) {
	for {
		select {
		case <-stopCh:
			Info.Print("Handler stopped")
			return
		default:
			evs := h.Receive()
			if len(evs) != 0 {
				h.GroupBy(&evs)
				h.Execute()
				continue
			}
			time.Sleep(HandlerSleep)
		}
	}
}

func (h *Handler) Run() {
	for {
		aliveFunc := h.etcd.SpinLock(h.key, h.ttl)
		if aliveFunc != nil {
			runWithMonitor(aliveFunc, h.syncHandleEvents)
		}
		time.Sleep(ErrorSleep)
	}
}

func NewHandler(etcd *Etcd, resource string, handleFunc func(task KV) bool, mergeFunc func(taskPrefix string)) Handler {
	return Handler{
		etcd:       etcd,
		resource:   resource,
		ttl:        10,
		key:        HandlerPrefix + resource + "/" + HostName() + Pid(),
		workers:    0,
		handleFunc: handleFunc,
		mergeFunc:  mergeFunc,
		done:       Ks{},
		doneLock:   new(sync.Mutex),
	}
}

func handleFunc(task KV) bool {
	return true
}

func RunHandle() {
	etcd := NewEtcd("127.0.0.1:2379", 20)
	defer etcd.client.Close()
	ha := NewHandler(&etcd, "apple", handleFunc, nil)
	ha.Run()
}
