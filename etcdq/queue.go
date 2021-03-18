/**
 * @Author: hiram
 * @Date: 2021/3/18 13:41
 */
package etcdq

type Q interface {
	enqueue(item string, stamp string, queueValue QueueValue) KV
	dequeue() KVs
}
type QueueValue struct {
	EventType Event_EventType
	Value     interface{}
}

type Queue struct {
	queuePrefix string
	etcd        *Etcd
}

func (q Queue) enqueue(item string, stamp string, queueValue QueueValue) KV {
	return KV{
		key:   q.queuePrefix + item + "-" + stamp,
		value: ObjectToString(queueValue),
	}
}
func (q Queue) dequeue() KVs {
	return q.etcd.ReadItems(q.queuePrefix)
}

var _ Q = new(Queue)

func NewQueue(etcd *Etcd, name string) Queue {
	return Queue{
		queuePrefix: QueuePrefix + name + "/",
		etcd:        etcd,
	}
}