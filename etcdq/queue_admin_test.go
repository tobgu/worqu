package etcdq_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tobgu/worqu/etcdq"
	"github.com/tobgu/worqu/tasks"
)

func newTestQueueAdmin() etcdq.QueueAdmin {
	return etcdq.NewQueueAdmin(etcdConfig, TestLogger{})
}

func TestQueueAdmin_CancelTask(t *testing.T) {
	qa := newTestQueueAdmin()
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	taskID, err := q.AddTask(TestData{Content: "TestCancelTask"})
	assert.NoError(t, err)

	err = qa.CancelTask("test-queue", taskID)
	assert.NoError(t, err)

	// Verify task was canceled
	tt, err := q.ListFinishedTasks(10)
	assert.NoError(t, err)
	assert.Len(t, tt, 1)
	assert.Equal(t, tasks.TaskStatusCancelled, tt[0].Status)
	assert.Equal(t, taskID, tt[0].ID)
}

func TestQueueAdmin_ListFinishedTasks(t *testing.T) {
	qa := newTestQueueAdmin()
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	taskID, err := q.AddTask(TestData{Content: "TestListFinishedTask"})
	assert.NoError(t, err)

	// Finish task to mark it as finished
	task, err := q.ClaimNextTask(context.Background())
	assert.NoError(t, err)
	err = q.FinishTask(task, tasks.TaskStatusSuccess)
	assert.NoError(t, err)

	finishedTasks, err := qa.ListFinishedTasks("test-queue")
	assert.NoError(t, err)
	assert.Len(t, finishedTasks, 1)
	assert.Equal(t, finishedTasks[0].ID, taskID)
	assert.Equal(t, tasks.TaskStatusSuccess, finishedTasks[0].Status)
}

func TestQueueAdmin_ListLiveTasks(t *testing.T) {
	qa := newTestQueueAdmin()
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	taskID, err := q.AddTask(TestData{Content: "TestListLiveTask"})
	assert.NoError(t, err)

	liveTasks, err := qa.ListLiveTasks("test-queue")
	assert.NoError(t, err)
	assert.Len(t, liveTasks, 1)
	assert.Equal(t, liveTasks[0].ID, taskID)
	assert.Equal(t, tasks.TaskStatusQueued, liveTasks[0].Status)
}

func TestQueueAdmin_ReQueueTask(t *testing.T) {
	qa := newTestQueueAdmin()
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	taskID, err := q.AddTask(TestData{Content: "TestReQueueTask"})
	assert.NoError(t, err)

	// Finish task to mark it as errored
	task, err := q.ClaimNextTask(context.Background())
	assert.NoError(t, err)
	err = q.FinishTask(task, tasks.TaskStatusErrored)
	assert.NoError(t, err)

	newTaskID, err := qa.ReQueueTask("test-queue", taskID)
	assert.NoError(t, err)
	assert.NotEqual(t, taskID, newTaskID)

	// Verify new task is re-queued
	liveTasks, err := qa.ListLiveTasks("test-queue")
	assert.NoError(t, err)
	assert.Len(t, liveTasks, 1)
	assert.Equal(t, newTaskID, liveTasks[0].ID)
	assert.Equal(t, tasks.TaskStatusQueued, liveTasks[0].Status)
}
