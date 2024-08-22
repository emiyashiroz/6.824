package mr

import "sync"

type DeQueue[T any] struct {
	lock sync.RWMutex
	arr  []T
}

func (q *DeQueue[T]) LeftPush(item T) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.arr = append([]T{item}, q.arr...)
}

func (q *DeQueue[T]) RightPoll() (T, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(q.arr) == 0 {
		return *new(T), false
	}
	item := q.arr[len(q.arr)-1]
	q.arr = q.arr[:len(q.arr)-1]
	return item, true
}

func (q *DeQueue[T]) LeftPoll() (T, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(q.arr) == 0 {
		return *new(T), false
	}
	item := q.arr[0]
	q.arr = q.arr[1:]
	return item, true
}

func (q *DeQueue[T]) RightPush(item T) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.arr = append(q.arr, item)
}
