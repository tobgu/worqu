package etcdq

import (
	"context"
	"errors"
	"fmt"
	"github.com/tobgu/worqu/log"
	"github.com/tobgu/worqu/tasks"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	concurrencyv3 "go.etcd.io/etcd/client/v3/concurrency"
	"strconv"
	"time"
)

func nowTimeStamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// Queue is a task queue using ETCD as a backing store.
//
// Layout in ETCD:
// /workqu/task-queues/<queue name>/live-tasks/<task id>
// - Tasks queued, and currently processing. Picked up in order of creation.
//
// /workqu/task-queues/<queue name>/processing-locks/<task id>
// - Locks for in progress tasks e.g. key -> ETCD Lease ID. Expires after a TTL if holding process dies unexpectedly.
//
// /workqu/task-queues/<queue name>/finished-tasks/<task id>
// - Tasks finished, either successfully, with errors, or cancelled.
//
// /workqu/task-queues/<queue name>/cancel-requests/<task id>
// - Processing tasks for which cancelling has been requested but not yet completed.
type Queue[T any] struct {
	name               string
	session            *concurrencyv3.Session
	kv                 clientv3.KV
	prefix             string
	leaseIDString      string
	finishedTaskTTL    int64
	repollInterval     time.Duration
	maxTaskConcurrency int // The maximum number of tasks allowed to process concurrently
	logger             log.Logger
}

// NewQueue returns a new queue configured according to the config passed in.
// The name argument denotes that name of the queue in ETCD and must be the
// same for producers and consumers of tasks that wish to interact.
func NewQueue[T any](name string, c Config, logger log.Logger) (*Queue[T], error) {
	client, err := NewClient(c)
	if err != nil {
		return nil, err
	}

	session, err := concurrencyv3.NewSession(client)
	if err != nil {
		return nil, fmt.Errorf("creating concurrent session against ETCD: %w", err)
	}

	return &Queue[T]{
		name:               name,
		session:            session,
		leaseIDString:      strconv.FormatInt(int64(session.Lease()), 16), // Base 16 to be compatible with etcdctl
		kv:                 clientv3.NewKV(client),
		prefix:             "/worqu/task-queues/" + name,
		finishedTaskTTL:    10 * 24 * 3600,
		repollInterval:     60 * time.Second,
		maxTaskConcurrency: c.MaxTaskConcurrency,
		logger:             logger,
	}, nil
}

func (q *Queue[T]) liveTasksPrefix() string {
	return q.prefix + "/live-tasks/"
}

func (q *Queue[T]) liveTasksKey(id tasks.TaskID) string {
	return q.liveTasksPrefix() + string(id)
}

func (q *Queue[T]) cancelRequestPrefix() string {
	return q.prefix + "/cancel-requests/"
}

func (q *Queue[T]) cancelRequestKey(id tasks.TaskID) string {
	return q.cancelRequestPrefix() + string(id)
}

func (q *Queue[T]) processingLockPrefix() string {
	return q.prefix + "/processing-locks/"
}

func (q *Queue[T]) processingLockKey(id tasks.TaskID) string {
	return q.processingLockPrefix() + string(id)
}

func (q *Queue[T]) finishedTasksPrefix() string {
	return q.prefix + "/finished-tasks/"
}

func (q *Queue[T]) finishedTasksKey(id tasks.TaskID) string {
	return q.finishedTasksPrefix() + string(id)
}

// AddTask adds a new task containing data to the queue.
func (q *Queue[T]) AddTask(data T) (tasks.TaskID, error) {
	task := tasks.Task[T]{
		Status:    tasks.TaskStatusQueued,
		CreatedTS: nowTimeStamp(),
		Data:      data,
	}
	taskBytes, err := task.Marshal()
	if err != nil {
		return "", fmt.Errorf("marshalling data: %w", err)
	}

	taskID := tasks.NewTaskID()
	_, err = q.kv.Put(context.Background(), q.liveTasksKey(taskID), string(taskBytes))
	if err != nil {
		return "", fmt.Errorf("adding task to queue: %w", err)
	}
	return taskID, nil
}

func (q *Queue[T]) tryClaimTask(ctx context.Context, kv *mvccpb.KeyValue) (*tasks.ClaimedTask[T], error) {
	key := string(kv.Key)
	taskID := tasks.TaskIDFromKey(key)
	lockKey := q.processingLockKey(taskID)
	taskKey := q.liveTasksKey(taskID)
	noLockCmp := clientv3.Compare(clientv3.Version(lockKey), "=", 0)
	// Task may have been cancelled since last seen
	tasksExistsCmp := clientv3.Compare(clientv3.Version(taskKey), "!=", 0)
	req := clientv3.OpPut(lockKey, q.leaseIDString, clientv3.WithLease(q.session.Lease()))
	resp, err := q.kv.Txn(ctx).If(noLockCmp, tasksExistsCmp).Then(req).Commit()
	if err != nil {
		return nil, fmt.Errorf("trying to claim lock for task %s: %w", lockKey, err)
	}

	if resp.Succeeded {
		task, err := tasks.UnMarshalTask[T](kv.Value)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling task %s: %w", key, err)
		}
		task.ID = taskID
		task.Status = tasks.TaskStatusProcessing
		task.StartedTS = nowTimeStamp()
		err = q.updateLiveTask(ctx, task)

		// Relay any cancel requests to the consumer through a dedicated channel
		ctx, cancelFunc := context.WithCancel(ctx)
		watchChan := q.session.Client().Watch(
			clientv3.WithRequireLeader(ctx),
			q.cancelRequestKey(taskID),
			clientv3.WithFilterDelete(),
			clientv3.WithRev(kv.CreateRevision),
		)

		cancelChannel := make(chan struct{})
		go func() {
			ev, ok := <-watchChan
			if !ok {
				return
			}
			if ev.Err() != nil {
				q.logger.Error("Cancel watch error: %v", ev.Err())
				return
			}
			close(cancelChannel)
		}()
		claimedTask := tasks.ClaimedTask[T]{
			Task:                task,
			CancelCancelWatchFn: cancelFunc,
			CancelChannel:       cancelChannel}
		return &claimedTask, err
	}

	return nil, nil
}

func (q *Queue[T]) updateLiveTask(ctx context.Context, task tasks.Task[T]) error {
	// N.B. Updating live tasks will potentially trigger live task
	// watches in other consumers and may be a tad wasteful. It should
	// work fine though since locking will make sure the other consumers
	// do not claim the task.
	bytes, err := task.Marshal()
	if err != nil {
		return fmt.Errorf("could not marshal updated live task: %w", err)
	}

	lockKey := q.processingLockKey(task.ID)
	cmp := clientv3.Compare(clientv3.Value(lockKey), "=", q.leaseIDString)
	liveKey := q.liveTasksKey(task.ID)
	putOp := clientv3.OpPut(liveKey, string(bytes))
	resp, err := q.kv.Txn(ctx).If(cmp).Then(putOp).Commit()
	if err != nil {
		return fmt.Errorf("writing updated task with key %s: %w", liveKey, err)
	}

	if !resp.Succeeded {
		return fmt.Errorf("updating live task with key %s, we don't own it: %w", liveKey, err)
	}

	return nil
}

// ClaimNextTask Returns the next task in line for processing and locks it so that
// it cannot be claimed by other consumers.
// If no tasks are available, ClaimNextTask will block until a task becomes available.
// To break execution the passed in context can be cancelled. The function will then
// return with an error which can be tested for with errors.Is(err, context.Canceled).
func (q *Queue[T]) ClaimNextTask(ctx context.Context) (*tasks.ClaimedTask[T], error) {
	for {
		task, err := q.claimNextTask(ctx)
		if task != nil || err != nil {
			return task, err
		}
	}
}

func (q *Queue[T]) claimNextTask(ctx context.Context) (*tasks.ClaimedTask[T], error) {
	// Read live-tasks in order, if elements exist try to claim lock with lease key as value and lease
	// If no tasks exist create a watch on the live-tasks (and a delete-watch on processing-tasks?) from
	// the revision number received in the empty response
	resp, err := q.kv.Get(
		ctx,
		q.liveTasksPrefix(),
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend),
		clientv3.WithLimit(2*int64(q.maxTaskConcurrency)))

	if err != nil {
		return nil, fmt.Errorf("claiming next task: %w", err)
	}

	for _, kv := range resp.Kvs {
		claimedTask, err := q.tryClaimTask(ctx, kv)
		if err != nil {
			return nil, err
		}

		if claimedTask != nil {
			return claimedTask, nil
		}
	}

	if len(resp.Kvs) > q.maxTaskConcurrency {
		// Sanity check to avoid buggy consumers that do not finish the tasks from
		// causing the queue to fill. This check may need to be refined.
		return nil, fmt.Errorf("claiming tasks, too many processing tasks detected")
	}

	// Watch for changes in live tasks to immediately try to pick up any
	// new task. There are corner cases such as consumers unexpectedly
	// dying that cause their locks to expire after a TTL. This
	// will not be picked up by the watch below. We hence allow it
	// to run for a maximum of 60 s before returning from this
	// function. This will cause it to be called again by the outer
	// function.
	waitCtx, cancelFunc := context.WithCancel(ctx)
	watchChan := q.session.Client().Watch(
		clientv3.WithRequireLeader(waitCtx),
		q.liveTasksPrefix(),
		clientv3.WithPrefix(),
		clientv3.WithFilterDelete(),
		clientv3.WithRev(resp.Header.Revision),
	)
	defer cancelFunc()

	timer := time.NewTimer(q.repollInterval)
	defer timer.Stop()
	for {
		select {
		case w := <-watchChan:
			if w.Err() != nil {
				return nil, fmt.Errorf("watching for new tasks, canceled = %t: %w", w.Canceled, err)
			}

			if len(w.Events) == 0 {
				return nil, fmt.Errorf("context cancelled while waiting for live keys: %w", waitCtx.Err())
			}

			for _, ev := range w.Events {
				claimedTask, err := q.tryClaimTask(ctx, ev.Kv)
				if err != nil {
					return nil, err
				}

				if claimedTask != nil {
					return claimedTask, nil
				}
			}
		case <-timer.C:
			return nil, nil
		}
	}
}

// ListLiveTasks lists live tasks (queued, processing). The result is sorted with
// the most recently queued tasks first.
func (q *Queue[T]) ListLiveTasks() ([]tasks.Task[T], error) {
	// List all tasks under live-tasks
	resp, err := q.kv.Get(
		context.Background(),
		q.liveTasksPrefix(),
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortDescend),
	)

	if err != nil {
		return nil, fmt.Errorf("failed getting live tasks: %w", err)
	}

	result := make([]tasks.Task[T], len(resp.Kvs))
	for i, kv := range resp.Kvs {
		task, err := tasks.UnMarshalTask[T](kv.Value)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling task with key %s: %w", string(kv.Key), err)
		}
		task.ID = tasks.TaskIDFromKey(string(kv.Key))
		result[i] = task
	}

	return result, nil
}

// ListFinishedTasks lists finished tasks (finished, errored, cancelled). Number of tasks returned
// is restricted by limit and is sorted with the most recently finished tasks last.
func (q *Queue[T]) ListFinishedTasks(limit int64) ([]tasks.Task[T], error) {
	// List all tasks under finished-tasks
	resp, err := q.kv.Get(
		context.Background(),
		q.finishedTasksPrefix(),
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortDescend),
		clientv3.WithLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("listing finished tasks: %w", err)
	}

	result := make([]tasks.Task[T], len(resp.Kvs))
	for i, kv := range resp.Kvs {
		task, err := tasks.UnMarshalTask[T](kv.Value)
		task.ID = tasks.TaskIDFromKey(string(kv.Key))
		if err != nil {
			return nil, fmt.Errorf("unmarshalling finished task with key %s: %w", string(kv.Key), err)
		}
		result[len(result)-i-1] = task
	}
	return result, nil
}

// Len returns the number of live tasks (queued + processing) in the queue.
func (q *Queue[T]) Len() (int, error) {
	// Length of live-tasks
	resp, err := q.kv.Get(
		context.Background(),
		q.liveTasksPrefix(),
		clientv3.WithPrefix(),
		clientv3.WithCountOnly())
	if err != nil {
		return 0, fmt.Errorf("getting queue length: %w", err)
	}
	return int(resp.Count), nil
}

// Shutdown closes the connection to ETCD and should be called when the queue will not be used anymore.
func (q *Queue[T]) Shutdown() error {
	// Explicit end of lease required?
	err := q.session.Close()
	if err != nil {
		return fmt.Errorf("shutting down queue %s: %w", q.name, err)
	}
	return nil
}

// ReQueueTask moves a finished task back to queued so that it can be claimed for processing again.
func (q *Queue[T]) ReQueueTask(id tasks.TaskID) (*tasks.Task[T], error) {
	finishedKey := q.finishedTasksKey(id)
	finishedTask, err := q.getTask(finishedKey)
	if err != nil {
		return nil, err
	}

	newTask := *finishedTask
	newTask.ID = tasks.NewTaskID()
	newTask.Status = tasks.TaskStatusQueued
	newTask.CreatedTS = nowTimeStamp()
	newTask.StartedTS = ""
	newTask.FinishedTS = ""

	newTaskBytes, err := newTask.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshalling new task when re-queueing task with ID %s: %w",
			finishedTask.ID, err)
	}

	tasksExistsCmp := clientv3.Compare(clientv3.Version(finishedKey), "!=", 0)
	deleteOp := clientv3.OpDelete(finishedKey)
	putOp := clientv3.OpPut(q.liveTasksKey(newTask.ID), string(newTaskBytes))
	resp, err := q.kv.Txn(context.Background()).If(tasksExistsCmp).Then(deleteOp, putOp).Commit()
	if err != nil {
		return nil, fmt.Errorf("re-queueing task with old ID %s and new ID %s: %w",
			finishedTask.ID, newTask.ID, err)
	}

	if !resp.Succeeded {
		return nil, fmt.Errorf("re-queueing task with ID %s does not exist", finishedTask.ID)
	}

	return &newTask, nil
}

func (q *Queue[T]) getTask(etcdKey string) (*tasks.Task[T], error) {
	resp, err := q.kv.Get(context.Background(), etcdKey)
	if err != nil {
		return nil, fmt.Errorf("getting task for key %s: %w", etcdKey, err)
	}

	if resp.Count != 1 {
		return nil, tasks.NewTaskNotFoundError("getting task with key %s, no such task", etcdKey)
	}

	task, err := tasks.UnMarshalTask[T](resp.Kvs[0].Value)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling task for key %s: %w", etcdKey, err)
	}

	task.ID = tasks.TaskIDFromKey(etcdKey)
	return &task, nil
}

func (q *Queue[T]) createFinishedPut(task tasks.Task[T], status tasks.TaskStatus) (clientv3.Op, error) {
	// Finished tasks are purged after some time
	finishedKey := q.finishedTasksKey(task.ID)
	finishLease, err := q.session.Client().Grant(context.Background(), q.finishedTaskTTL)
	if err != nil {
		return clientv3.Op{}, fmt.Errorf("getting lease for finished task: %w", err)
	}

	task.Status = status
	task.FinishedTS = nowTimeStamp()
	taskBytes, err := task.Marshal()
	if err != nil {
		return clientv3.Op{}, fmt.Errorf("marshalling finished event: %w", err)
	}

	return clientv3.OpPut(finishedKey, string(taskBytes), clientv3.WithLease(finishLease.ID)), nil
}

// FinishTask moves a processing task to finished state setting the status passed in.
func (q *Queue[T]) FinishTask(task *tasks.ClaimedTask[T], status tasks.TaskStatus) error {
	if !tasks.IsFinishedStatus(status) {
		return tasks.NewInputError("invalid finished status provided: %s", status)
	}
	putOp, err := q.createFinishedPut(task.Task, status)
	if err != nil {
		return err
	}
	lockKey := q.processingLockKey(task.ID)
	liveKey := q.liveTasksKey(task.ID)
	cmp := clientv3.Compare(clientv3.Value(lockKey), "=", q.leaseIDString)
	deleteOp := clientv3.OpDelete(liveKey)
	releaseOp := clientv3.OpDelete(lockKey)
	cancelKey := q.cancelRequestKey(task.ID)
	deleteCancelOp := clientv3.OpDelete(cancelKey) // May exist so we try to delete it
	resp, err := q.kv.Txn(context.Background()).If(cmp).Then(putOp, deleteOp, releaseOp, deleteCancelOp).Commit()
	if err != nil {
		return fmt.Errorf("moving task to finished for id %s: %w", task.ID, err)
	}

	if !resp.Succeeded {
		return tasks.NewInputError("cannot finish task %s, not owned by us", task.ID)
	}

	task.CancelCancelWatchFn()
	return nil
}

// CancelTask cancels a live task.
//
// If the tasks is queued it is immediately moved to finished tasks with status set to cancelled.
// If the task is processing the consumer is notified that the task has been cancelled and can act on it.
func (q *Queue[T]) CancelTask(id tasks.TaskID) error {
	// Remove task from live-tasks if no lock exists
	// Add entry to cancel-requests given that task exists in live-tasks
	liveKey := q.liveTasksKey(id)
	task, err := q.getTask(liveKey)
	if err != nil {
		return err
	}
	putOp, err := q.createFinishedPut(*task, tasks.TaskStatusCancelled)
	if err != nil {
		return err
	}

	lockKey := q.processingLockKey(id)
	tasksExistsCmp := clientv3.Compare(clientv3.Version(liveKey), "!=", 0)
	noLockCmp := clientv3.Compare(clientv3.Version(lockKey), "=", 0)
	deleteOp := clientv3.OpDelete(liveKey)
	resp, err := q.kv.Txn(context.Background()).
		If(noLockCmp, tasksExistsCmp).
		Then(deleteOp, putOp).
		Commit()
	if err != nil {
		return fmt.Errorf("trying to cancel task: %w", err)
	}

	if resp.Succeeded {
		// Task has not started processing yet, we're done
		return nil
	}

	// If the task is already processing we must request a cancellation
	// by the consumer in question.
	// We do this by adding a cancelRequest, which is monitored by the
	// consumer who can choose to act on it.
	cancelKey := q.cancelRequestKey(id)
	_, err = q.kv.Put(context.Background(), cancelKey, q.leaseIDString)
	if err != nil {
		return fmt.Errorf("making cancel request: %w", err)
	}

	return nil
}

func (q *Queue[T]) GetTask(id tasks.TaskID) (*tasks.Task[T], error) {
	task, err := q.getTask(q.liveTasksKey(id))
	if err == nil {
		return task, nil
	}

	if !errors.As(err, &tasks.TaskNotFoundError{}) {
		return nil, err
	}

	return q.getTask(q.finishedTasksKey(id))
}
