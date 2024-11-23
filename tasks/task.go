package tasks

import (
	"encoding/json"
	"github.com/gofrs/uuid"
	"strings"
)

// TaskID is the ID of the task. It is generated by the queue
// upon adding the task to it.
type TaskID string

func TaskIDFromKey(etcdKey string) TaskID {
	lastIx := strings.LastIndex(etcdKey, "/")
	return TaskID(etcdKey[lastIx+1:])
}

func NewTaskID() TaskID {
	id, err := uuid.NewV1()
	if err != nil {
		panic(err)
	}
	return TaskID(id.String())
}

// TaskStatus denotes the state the task is currently in. It's mostly for
// information and transparency. The queue does not make any decisions based
// on the status field of the Task.
type TaskStatus string

const (
	TaskStatusQueued     TaskStatus = "QUEUED"
	TaskStatusProcessing TaskStatus = "PROCESSING"
	TaskStatusSuccess    TaskStatus = "SUCCESS"
	TaskStatusErrored    TaskStatus = "ERRORED"
	TaskStatusCancelled  TaskStatus = "CANCELLED"
)

func IsFinishedStatus(status TaskStatus) bool {
	return status == TaskStatusSuccess || status == TaskStatusErrored || status == TaskStatusCancelled
}

// Task contains some metadata about a task and a generic data
// container that can hold data specific to the use case.
// All fields in Data must be public and JSON serializable.
type Task[T any] struct {
	ID         TaskID     `json:"id"`
	Status     TaskStatus `json:"status"`
	CreatedTS  string     `json:"created_ts"`
	StartedTS  string     `json:"started_ts"`
	FinishedTS string     `json:"finished_ts"`
	Data       T          `json:"data"`
}

func (t *Task[T]) Marshal() ([]byte, error) {
	return json.Marshal(t)
}

func UnMarshalTask[T any](bb []byte) (Task[T], error) {
	var t Task[T]
	err := json.Unmarshal(bb, &t)
	return t, err
}

// ClaimedTask is a Task returned by when claiming a task for processing.
// In addition to the Task itself it contains a cancel channel which can
// be used to detect task cancellations.
type ClaimedTask[T any] struct {
	Task[T]
	CancelCancelWatchFn func()
	CancelChannel       <-chan struct{}
}
