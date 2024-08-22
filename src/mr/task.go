package mr

import "time"

const (
	TaskTypeMap    = "Map"
	TaskTypeReduce = "Reduce"
)

type Task struct {
	co       *Coordinator
	Type     string
	FileName string
	doneCh   chan struct{}
	taskID   int
	workerID int
}

func (t *Task) done() {
	t.doneCh <- struct{}{}
}

func (t *Task) invoke() {
	go func() {
		select {
		case <-t.doneCh:
			t.co.done()
			if t.Type == TaskTypeMap {
				t.co.wgMap.Done()
			}
		case <-time.After(time.Second * 10):
			nt := Task{
				co:       t.co,
				Type:     t.Type,
				FileName: t.FileName,
				doneCh:   make(chan struct{}),
				taskID:   t.taskID,
			}
			nt.co.taskMap[nt.taskID] = &nt
			nt.co.taskQueue.LeftPush(t.taskID)
		}
	}()
}

func NewTask(t, f string, taskID int, co *Coordinator) *Task {
	return &Task{
		co:       co,
		Type:     t,
		FileName: f,
		doneCh:   make(chan struct{}),
		taskID:   taskID,
	}
}
