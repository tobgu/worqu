package etcdq_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tobgu/worqu/etcdq"
	"github.com/tobgu/worqu/tasks"
	"regexp"
	"testing"
	"time"
)

func TestQueue_BasicInsertGetFinishScenario(t *testing.T) {
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	testData := TestData{Content: "Hello"}
	newTaskID := addTask(t, q, testData)

	l, err := q.Len()
	assert.NoError(t, err)
	assert.Equal(t, 1, l)

	tt, err := q.ListLiveTasks()
	assert.NoError(t, err)
	assert.Len(t, tt, 1)
	assert.Equal(t, tasks.TaskStatusQueued, tt[0].Status)
	assert.NotEmpty(t, tt[0].CreatedTS)

	claimedTask, err := q.ClaimNextTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, testData, claimedTask.Data)
	assert.Equal(t, newTaskID, claimedTask.ID)

	tt, err = q.ListLiveTasks()
	assert.NoError(t, err)
	assert.Len(t, tt, 1)
	assert.Equal(t, tasks.TaskStatusProcessing, tt[0].Status)
	assert.NotEmpty(t, tt[0].StartedTS)

	err = q.FinishTask(claimedTask, tasks.TaskStatusSuccess)
	assert.NoError(t, err)

	l, err = q.Len()
	assert.NoError(t, err)
	assert.Equal(t, 0, l)

	tt, err = q.ListFinishedTasks(10)
	assert.NoError(t, err)
	assert.Len(t, tt, 1)
	assert.Equal(t, claimedTask.Data, tt[0].Data)
	assert.Equal(t, tasks.TaskStatusSuccess, tt[0].Status)
	assert.NotEmpty(t, tt[0].FinishedTS)
}

func TestQueue_TasksPutOnQueueAfterConsumerStarted(t *testing.T) {
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	testData := TestData{Content: "HelloThere"}
	coordinationChan := make(chan bool)
	go func() {
		coordinationChan <- true
		task, err := q.ClaimNextTask(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, testData, task.Data)
		close(coordinationChan)
	}()
	<-coordinationChan
	time.Sleep(100 * time.Millisecond)
	addTask(t, q, testData)
	<-coordinationChan
	tt, err := q.ListLiveTasks()
	assert.NoError(t, err)
	assert.Len(t, tt, 1)
	assert.Equal(t, tasks.TaskStatusProcessing, tt[0].Status)
	assert.NotEmpty(t, tt[0].StartedTS)
}

func TestQueue_MultipleConsumersAndProducers(t *testing.T) {
	// Setup: 1 producer of 10 tasks -> 10 consumers -> 10 producers -> 1 consumer of 10 tasks
	// Verify that all tasks are processed
	consumerCount := 10
	inputQueueInstances := make([]*etcdq.Queue[TestData], consumerCount)
	outputQueueInstances := make([]*etcdq.Queue[TestData], consumerCount)

	// Setup multiple instances of queues to simulate truly distinct clients
	for i := range inputQueueInstances {
		q, err := etcdq.NewQueue[TestData]("input-queue", etcdConfig, TestLogger{})
		assert.NoError(t, err)
		inputQueueInstances[i] = q
		q, err = etcdq.NewQueue[TestData]("output-queue", etcdConfig, TestLogger{})
		assert.NoError(t, err)
		outputQueueInstances[i] = q
	}

	// Pre cleanup
	_, err := inputQueueInstances[0].Clear()
	assert.NoError(t, err)
	_, err = outputQueueInstances[0].Clear()
	assert.NoError(t, err)

	// Post cleanup
	defer func() {
		for i := 0; i < consumerCount; i++ {
			assert.NoError(t, inputQueueInstances[i].Shutdown())
			assert.NoError(t, outputQueueInstances[i].Shutdown())
		}
	}()

	// 10 consumers of input + 10 producers of output
	for i := 0; i < consumerCount; i++ {
		go func(id int) {
			task, err := inputQueueInstances[id].ClaimNextTask(context.Background())
			assert.NoError(t, err)

			task.Data.Content = fmt.Sprintf("%d - %s", id, task.Data.Content)
			addTask(t, outputQueueInstances[id], task.Data)

			err = inputQueueInstances[id].FinishTask(task, tasks.TaskStatusSuccess)
			assert.NoError(t, err)
		}(i)
	}

	// 1 producer of input
	t0 := time.Now()
	for i := 0; i < consumerCount; i++ {
		addTask(t, inputQueueInstances[0], TestData{Content: fmt.Sprintf("%d", i)})
	}
	println("produce input:", time.Since(t0).Milliseconds(), "ms")

	// 1 consumer of output
	t0 = time.Now()
	outputSet := make(map[string]struct{})
	re := regexp.MustCompile(`\d - \d`)
	for i := 0; i < consumerCount; i++ {
		task, err := outputQueueInstances[0].ClaimNextTask(context.Background())
		assert.NoError(t, err)
		assert.Regexp(t, re, task.Data.Content)
		err = outputQueueInstances[0].FinishTask(task, tasks.TaskStatusSuccess)
		assert.NoError(t, err)
		outputSet[task.Data.Content] = struct{}{}
	}
	println("consume output:", time.Since(t0).Milliseconds(), "ms")
	assert.Len(t, outputSet, consumerCount)
}

func TestQueue_ConsumersNotFinishingTasksEventuallyCauseClaimingOfNewTasksToFail(t *testing.T) {
	// This is a safeguard to find misbehaving consumers that would otherwise
	// start filling up the queue.
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	taskCount := 30
	for i := 0; i < taskCount; i++ {
		addTask(t, q, TestData{Content: fmt.Sprintf("%d", i)})
	}

	var err error
	for i := 0; i < taskCount; i++ {
		_, err = q.ClaimNextTask(context.Background())
		// Misbehave by not finishing task before accepting a new one
	}
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too many processing tasks detected")
}

func TestQueue_TasksConsumedInInsertOrder(t *testing.T) {
	// This is a safeguard to find misbehaving consumers that would otherwise
	// start filling up the queue.
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	taskCount := 30
	for i := 0; i < taskCount; i++ {
		addTask(t, q, TestData{Content: fmt.Sprintf("%d", i)})
	}

	for i := 0; i < taskCount; i++ {
		task, err := q.ClaimNextTask(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, TestData{Content: fmt.Sprintf("%d", i)}, task.Data)
		err = q.FinishTask(task, tasks.TaskStatusSuccess)
		assert.NoError(t, err)
	}
}

func TestQueue_CancelNotYetStartedTask(t *testing.T) {
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	taskID := addTask(t, q, TestData{Content: "Hello"})

	err := q.CancelTask(taskID)
	assert.NoError(t, err)

	tt, err := q.ListLiveTasks()
	assert.NoError(t, err)
	assert.Empty(t, tt)

	tt, err = q.ListFinishedTasks(10)
	assert.NoError(t, err)
	assert.Len(t, tt, 1)
	assert.Equal(t, taskID, tt[0].ID)
	assert.Equal(t, tasks.TaskStatusCancelled, tt[0].Status)
}

func TestQueue_CancelProcessingTaskClaimedByScan(t *testing.T) {
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	taskID := addTask(t, q, TestData{Content: "Hello"})

	task, err := q.ClaimNextTask(context.Background())
	assert.NoError(t, err)

	err = q.CancelTask(taskID)
	assert.NoError(t, err)

	var cancelled bool
	select {
	case <-task.CancelChannel:
		cancelled = true
		break
	case <-time.After(5 * time.Second):
		cancelled = false
		break
	}
	assert.True(t, cancelled)

	err = q.FinishTask(task, tasks.TaskStatusCancelled)
	assert.NoError(t, err)

	tt, err := q.ListLiveTasks()
	assert.NoError(t, err)
	assert.Empty(t, tt)

	tt, err = q.ListFinishedTasks(10)
	assert.NoError(t, err)
	assert.Len(t, tt, 1)
	assert.Equal(t, taskID, tt[0].ID)
	assert.Equal(t, tasks.TaskStatusCancelled, tt[0].Status)
}

func TestQueue_CancelProcessingTaskClaimedByWatch(t *testing.T) {
	// The expected behaviour in this test case is the same as
	// for TestQueueCancelProcessingTaskClaimedByScan but the code
	// paths are slightly different for which may introduce unwanted
	// differences in handling.
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	testData := TestData{Content: "Hello"}
	coordinationChan := make(chan bool)
	go func() {
		coordinationChan <- true
		task, err := q.ClaimNextTask(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, testData, task.Data)
		select {
		case <-task.CancelChannel:
			err = q.FinishTask(task, tasks.TaskStatusCancelled)
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("Timed out waiting for task cancel")
		}
		close(coordinationChan)
	}()
	<-coordinationChan
	time.Sleep(100 * time.Millisecond)
	taskID := addTask(t, q, testData)
	time.Sleep(100 * time.Millisecond)
	err := q.CancelTask(taskID)
	assert.NoError(t, err)
	<-coordinationChan

	tt, err := q.ListFinishedTasks(10)
	assert.NoError(t, err)
	assert.Len(t, tt, 1)
	assert.Equal(t, taskID, tt[0].ID)
	assert.Equal(t, tasks.TaskStatusCancelled, tt[0].Status)
}

func TestQueue_CannotFinishTasksThatAreNotClaimedByClient(t *testing.T) {
	q1, shutdownFn1 := newTestQueue(t)
	defer shutdownFn1()
	q2, shutdownFn2 := newTestQueue(t)
	defer shutdownFn2()

	addTask(t, q1, TestData{Content: "HelloQ1"})

	task, err := q1.ClaimNextTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "HelloQ1", task.Data.Content)

	err = q2.FinishTask(task, tasks.TaskStatusSuccess)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not owned by us")
}

func TestQueue_TaskWithNoLockCanBeReclaimed(t *testing.T) {
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	// Change from default 60 s to avoid that the test hangs for a minute
	q.SetRepollInterval(10 * time.Millisecond)

	addTask(t, q, TestData{Content: "HelloQ1"})

	task1, err := q.ClaimNextTask(context.Background())
	assert.NoError(t, err)

	coordinationChan := make(chan bool)
	go func() {
		coordinationChan <- true
		task2, err := q.ClaimNextTask(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, task1.ID, task2.ID)
		close(coordinationChan)
	}()
	<-coordinationChan
	select {
	case <-coordinationChan:
		assert.Fail(t, "Unexpected claim of locked task")
	case <-time.After(100 * time.Millisecond):
		// Delete lock to trigger task to be picked up by go routine above.
		client, err := etcdq.NewClient(etcdConfig)
		assert.NoError(t, err)
		resp, err := client.Delete(context.Background(), "/worqu/task-queues/test-queue/processing-locks/"+string(task1.ID))
		assert.NoError(t, err)
		assert.Equal(t, int64(1), resp.Deleted)
	}
	<-coordinationChan
}

func TestReQueueingFinishedTask(t *testing.T) {
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	addTask(t, q, TestData{Content: "Hello"})

	task, err := q.ClaimNextTask(context.Background())
	assert.NoError(t, err)

	err = q.FinishTask(task, tasks.TaskStatusErrored)
	assert.NoError(t, err)

	newTask, err := q.ReQueueTask(task.ID)
	assert.NoError(t, err)
	assert.NotEqual(t, task.ID, newTask.ID)
	assert.Equal(t, task.Data, newTask.Data)

	tt, err := q.ListLiveTasks()
	assert.NoError(t, err)
	assert.Equal(t, newTask.ID, tt[0].ID)
	assert.Equal(t, newTask.Data, tt[0].Data)
	assert.Equal(t, tasks.TaskStatusQueued, tt[0].Status)
	assert.Equal(t, "", tt[0].StartedTS)
	assert.Equal(t, "", tt[0].FinishedTS)

	claimedTask, err := q.ClaimNextTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, newTask.ID, claimedTask.ID)
}

func TestQueue_CancelClaimNextTask(t *testing.T) {
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	coordinationChan := make(chan bool)
	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		coordinationChan <- true
		task, err := q.ClaimNextTask(ctx)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, task)
		close(coordinationChan)
	}()
	<-coordinationChan
	time.Sleep(100 * time.Millisecond)
	cancelFunc()
	<-coordinationChan
}

func TestQueue_CancelBeforeClaimNextTask(t *testing.T) {
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	addTask(t, q, TestData{Content: "Hello"})

	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()

	// Don't expect anything to be received when context has been
	// cancelled even though queued task exist.
	task, err := q.ClaimNextTask(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, task)
}

func TestQueue_GetLiveTask(t *testing.T) {
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	id := addTask(t, q, TestData{Content: "Hello"})

	task, err := q.GetTask(id)
	assert.NoError(t, err)
	assert.Equal(t, id, task.ID)
}

func TestQueue_GetFinishedTask(t *testing.T) {
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	id := addTask(t, q, TestData{Content: "Hello"})

	claimedTask, err := q.ClaimNextTask(context.Background())
	assert.NoError(t, err)

	err = q.FinishTask(claimedTask, tasks.TaskStatusErrored)
	assert.NoError(t, err)

	task, err := q.GetTask(id)
	assert.NoError(t, err)
	assert.Equal(t, id, task.ID)
	assert.Equal(t, tasks.TaskStatusErrored, task.Status)
}

func TestQueue_TryGetMissingTask(t *testing.T) {
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	_, err := q.GetTask("foo")
	assert.Error(t, err)
}

func TestQueue_CancelFinishedTaskReturnsTaskNotFound(t *testing.T) {
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	data := TestData{Content: "Task to be finished"}
	taskID := addTask(t, q, data)

	claimedTask, err := q.ClaimNextTask(context.Background())
	assert.NoError(t, err)
	err = q.FinishTask(claimedTask, tasks.TaskStatusSuccess)
	assert.NoError(t, err)

	err = q.CancelTask(taskID)
	assert.Error(t, err)
	assert.ErrorAs(t, err, &tasks.TaskNotFoundError{})
}

// TODO: Long running test with resource monitoring to identify any
//       type of leakage, etc.
