package etcdq

import (
	"github.com/tobgu/worqu/log"
	"github.com/tobgu/worqu/tasks"
)

type QueueAdmin struct {
	config Config
	logger log.Logger
}

func NewQueueAdmin(c Config, logger log.Logger) QueueAdmin {
	return QueueAdmin{config: c, logger: logger}
}

func (qa QueueAdmin) CancelTask(queueName string, id tasks.TaskID) error {
	q, err := NewQueue[any](queueName, qa.config, qa.logger)
	if err != nil {
		return err
	}
	defer qa.shutdown(q)
	return q.CancelTask(id)
}

func (qa QueueAdmin) ListFinishedTasks(queueName string) ([]tasks.Task[any], error) {
	q, err := NewQueue[any](queueName, qa.config, qa.logger)
	if err != nil {
		return nil, err
	}
	defer qa.shutdown(q)

	// Arbitrary limit at the moment, make it possible to pass in as variable if needed.
	return q.ListFinishedTasks(5000)
}

func (qa QueueAdmin) ListLiveTasks(queueName string) ([]tasks.Task[any], error) {
	q, err := NewQueue[any](queueName, qa.config, qa.logger)
	if err != nil {
		return nil, err
	}
	defer qa.shutdown(q)
	return q.ListLiveTasks()
}

func (qa QueueAdmin) ReQueueTask(queueName string, id tasks.TaskID) (tasks.TaskID, error) {
	q, err := NewQueue[any](queueName, qa.config, qa.logger)
	if err != nil {
		return "", err
	}
	defer qa.shutdown(q)
	task, err := q.ReQueueTask(id)
	if err != nil {
		return "", err
	}
	return task.ID, err
}

func (qa QueueAdmin) shutdown(q *Queue[any]) {
	// Explicitly ignore errors from shutdown to satisfy linter
	err := q.Shutdown()
	if err != nil {
		qa.logger.Error("shutting down queue in QueueAdmin: %v", err)
	}
}
