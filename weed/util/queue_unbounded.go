package util

import "sync"

type UnboundedQueue struct {
	outbound     []string
	outboundLock sync.RWMutex
	inbound      []string
	inboundLock  sync.RWMutex
}

/// 模拟 入队 和 批量消费, 线程安全
func NewUnboundedQueue() *UnboundedQueue {
	q := &UnboundedQueue{}
	return q
}

func (q *UnboundedQueue) EnQueue(items ...string) {
	q.inboundLock.Lock()
	defer q.inboundLock.Unlock()

	q.inbound = append(q.inbound, items...)

}

/// 批量消费
func (q *UnboundedQueue) Consume(fn func([]string)) {
	q.outboundLock.Lock()
	defer q.outboundLock.Unlock()

	if len(q.outbound) == 0 {
		q.inboundLock.Lock()
		inbountLen := len(q.inbound)
		if inbountLen > 0 {
			t := q.outbound
			q.outbound = q.inbound
			q.inbound = t
		}
		q.inboundLock.Unlock()
	}

	if len(q.outbound) > 0 {
		fn(q.outbound)
		q.outbound = q.outbound[:0]
	}

}
