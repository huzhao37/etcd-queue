package etcdq

import (
	"context"
	v3 "go.etcd.io/etcd/clientv3"
	"time"
)

type Client interface {
	ReadItem(string) KV
	ReadItems(string) KVs
	WriteItem(string, string)
	DeleteItem(string)
	Atomic(writes KVs, deletes Ks) *v3.TxnResponse
	WatchPrefix(prefix string, startRevision int64, eventCh chan Events, stopCh chan struct{})
	SpinLock(name string, ttl int64) func() bool // A distributed lock, return func for lockAliveCheck
}

type Event struct {
	eventType Event_EventType
	revision  int64
	key       string
	value     string
	stamp     int64
}

type Events []Event
type KV struct {
	key   string
	value string
}

var emptyKV = KV{}

type KVs []KV
type Ks []string
type Etcd struct {
	client      *v3.Client
	endPoint    string
	dialTimeout time.Duration
	MaxTxnOps   int
}

var _ Client = new(Etcd)

func (e *Etcd) ReadItem(key string) KV {
	resp, err := e.client.Get(ctx(), key)
	if err != nil {
		Error.Println(err)
		return emptyKV
	}
	results := resp.Kvs
	if len(results) == 0 {
		return emptyKV
	}
	return KV{
		key:   string(results[0].Key),
		value: string(results[0].Value),
	}
}
func (e *Etcd) ReadItems(key string) KVs {
	resp, err := e.client.Get(ctx(), key, v3.WithPrefix())
	if err != nil {
		Error.Println(err)
		return KVs{}
	}
	var kvs KVs
	for _, result := range resp.Kvs {
		kvs = append(kvs, KV{
			key:   string(result.Key),
			value: string(result.Value),
		})
	}
	return kvs
}
func (e *Etcd) WriteItem(key string, value string) {
	_, err := e.client.Put(ctx(), key, value)
	if err != nil {
		Error.Println(err)
	}
}
func (e *Etcd) DeleteItem(key string) {
	_, err := e.client.Delete(ctx(), key)
	if err != nil {
		Error.Println(err)
	}
}
func (e *Etcd) Atomic(writes KVs, deletes Ks) *v3.TxnResponse {
	//Debug.Println("writes len", len(writes))
	txn := e.client.Txn(ctx())
	var ops []v3.Op
	for _, w := range writes {
		ops = append(ops, v3.OpPut(w.key, w.value))
	}
	for _, key := range deletes {
		ops = append(ops, v3.OpDelete(key))
	}
	txn.Then(ops...)
	resp, err := txn.Commit()
	if err != nil {
		Error.Println(err)
		//time.Sleep(ErrorSleep)
		//return e.Atomic(writes, deletes)
	}
	return resp
}

func (e *Etcd) WatchPrefix(prefix string, startRevision int64, eventCh chan Events, stopCh chan struct{}) {
	watchChan := e.client.Watch(context.Background(), prefix, v3.WithPrefix(), v3.WithRev(startRevision), v3.WithPrevKV())
	for {
		select {
		case <-stopCh:
			Info.Print("Watch cancelled, Event channel closed")
			close(eventCh)
			return
		case resp, ok := <-watchChan:
			if !ok {
				Info.Print("Watch interrupted, Event channel closed")
				close(eventCh)
				return
			}
			var events Events
			for _, ev := range resp.Events {
				events = append(events, Event{
					eventType: Event_EventType(ev.Type),
					revision:  ev.Kv.ModRevision,
					key:       string(ev.Kv.Key),
					value:     string(ev.Kv.Value),
					stamp:     Stamp(),
				})
			}
			eventCh <- events
		}
	}
}
func (e *Etcd) getLease(ttl int64) (v3.LeaseID, error) {
	lease := v3.NewLease(e.client)
	var (
		leaseKeepAliveChan <-chan *v3.LeaseKeepAliveResponse
		leaseId            v3.LeaseID
	)
	leaseGrantResponse, err := lease.Grant(context.TODO(), ttl)
	if err != nil {
		return leaseId, err
	}
	leaseId = leaseGrantResponse.ID
	leaseKeepAliveChan, err = lease.KeepAlive(context.TODO(), leaseId)
	if err != nil {
		return leaseId, err
	}
	go func() {
		for {
			select {
			case resp := <-leaseKeepAliveChan:
				if resp == nil {
					Debug.Println("lease dead")
					return
				}
			}
		}
	}()
	return leaseId, nil
}
func (e *Etcd) SpinLock(name string, ttl int64) func() bool {
	leaseId, err := e.getLease(ttl)
	if err != nil {
		Error.Println(err)
		return nil
	}
	lockKey := LockPrefix + name
	Info.Println("Wait lock: ", lockKey)
	for {
		txn := e.client.Txn(context.TODO())
		resp, err := txn.If(v3.Compare(v3.CreateRevision(lockKey), "=", 0)).
			Then(v3.OpPut(lockKey, "single", v3.WithLease(leaseId))).
			Else(v3.OpGet(lockKey)).
			Commit()
		if err != nil {
			Error.Println(err)
			return nil
		}
		if !resp.Succeeded {
			time.Sleep(time.Second * time.Duration(ttl/2))
			continue
		}
		// Alive func is used to monitor connection's health
		Info.Println("Get lock: ", lockKey)
		return func() bool {
			kv := e.ReadItem(lockKey)
			return emptyKV != kv
		}
	}
}
func NewEtcd(endPoint string, dialTimeout time.Duration) Etcd {
	etcd := Etcd{
		endPoint:    endPoint,
		dialTimeout: dialTimeout,
		MaxTxnOps:   MaxTxnOps,
	}
	client, err := v3.New(v3.Config{
		Endpoints:          []string{etcd.endPoint},
		DialTimeout:        etcd.dialTimeout * time.Second,
		MaxCallSendMsgSize: MaxCallSendMsgSize,
		MaxCallRecvMsgSize: MaxCallRecvMsgSize,
	})
	if err != nil {
		Error.Println(err)
	}
	etcd.client = client
	return etcd
}