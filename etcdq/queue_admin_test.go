package etcdq_test

import (
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

	taskID := addTask(t, q, TestData{Content: "TestCancelTask"})

	err := qa.CancelTask("test-queue", taskID)
	assert.NoError(t, err)

	// Verify task was canceled
	tt := listFinishedTasks(t, q, 10)
	assert.Len(t, tt, 1)
	assert.Equal(t, tasks.TaskStatusCancelled, tt[0].Status)
	assert.Equal(t, taskID, tt[0].ID)
}

func TestQueueAdmin_ListFinishedTasks(t *testing.T) {
	qa := newTestQueueAdmin()
	q, shutdownFn := newTestQueue(t)
	defer shutdownFn()

	taskID := addTask(t, q, TestData{Content: "TestListFinishedTask"})

	// Finish task to mark it as finished
	task := claimNextTask(t, q)
	finishTask(t, q, task, tasks.TaskStatusSuccess)

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

	taskID := addTask(t, q, TestData{Content: "TestListLiveTask"})

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

	taskID := addTask(t, q, TestData{Content: "TestReQueueTask"})

	// Finish task to mark it as errored
	task := claimNextTask(t, q)
	finishTask(t, q, task, tasks.TaskStatusErrored)

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
