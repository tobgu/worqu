package etcdq_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tobgu/worqu/etcdq"
	"github.com/tobgu/worqu/tasks"
	"testing"
)

type TestLogger struct{}

func (t TestLogger) Debug(format string, v ...any) {
	t.Info(format, v...)
}

func (t TestLogger) Info(format string, v ...any) {
	fmt.Printf(format, v...)
}

func (t TestLogger) Warn(format string, v ...any) {
	t.Info(format, v...)
}

func (t TestLogger) Error(format string, v ...any) {
	t.Info(format, v...)
}

type TestData struct {
	Content string
}

var etcdConfig = etcdq.Config{
	EtcdHosts:          "localhost:2379",
	MaxTaskConcurrency: 5,
	EtcdTimeoutSeconds: 5,
	TLSCertFile:        "../test-certs/cert.pem",
	TLSKeyFile:         "../test-certs/key.pem",
	TLSCACertFile:      "../test-certs/ca.pem",
}

func newTestQueue(t *testing.T) (*etcdq.Queue[TestData], func()) {
	t.Helper()
	q, err := etcdq.NewQueue[TestData]("test-queue", etcdConfig, TestLogger{})
	assert.NoError(t, err)
	_, err = q.Clear()
	assert.NoError(t, err)
	return q, func() {
		assert.NoError(t, q.Shutdown())
	}
}

func addTask(t *testing.T, q *etcdq.Queue[TestData], testData TestData) tasks.TaskID {
	t.Helper()
	newTaskID, err := q.AddTask(testData)
	assert.NoError(t, err)
	return newTaskID
}

func claimNextTask(t *testing.T, q *etcdq.Queue[TestData]) *tasks.ClaimedTask[TestData] {
	t.Helper()
	claimedTask, err := q.ClaimNextTask(context.Background())
	assert.NoError(t, err)
	return claimedTask
}

func listFinishedTasks(t *testing.T, q *etcdq.Queue[TestData], limit int64) []tasks.Task[TestData] {
	t.Helper()
	tt, err := q.ListFinishedTasks(limit)
	assert.NoError(t, err)
	return tt
}

func finishTask(
	t *testing.T,
	q *etcdq.Queue[TestData],
	claimedTask *tasks.ClaimedTask[TestData],
	status tasks.TaskStatus) {
	t.Helper()
	err := q.FinishTask(claimedTask, status)
	assert.NoError(t, err)
}

func listLiveTasks(t *testing.T, q *etcdq.Queue[TestData]) []tasks.Task[TestData] {
	t.Helper()
	tt, err := q.ListLiveTasks()
	assert.NoError(t, err)
	return tt
}

func qLen(t *testing.T, q *etcdq.Queue[TestData]) int {
	t.Helper()
	l, err := q.Len()
	assert.NoError(t, err)
	return l
}

func clearQ(t *testing.T, q *etcdq.Queue[TestData]) int64 {
	t.Helper()
	count, err := q.Clear()
	assert.NoError(t, err)
	return count
}

func cancelTask(t *testing.T, q *etcdq.Queue[TestData], taskID tasks.TaskID) {
	t.Helper()
	err := q.CancelTask(taskID)
	assert.NoError(t, err)
}

func getTask(t *testing.T, q *etcdq.Queue[TestData], id tasks.TaskID) *tasks.Task[TestData] {
	t.Helper()
	task, err := q.GetTask(id)
	assert.NoError(t, err)
	return task
}
