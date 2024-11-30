package etcdq_test

import (
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
	newTaskID, err := q.AddTask(testData)
	assert.NoError(t, err)
	return newTaskID
}
