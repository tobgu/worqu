//go:build test_tweaks

// This file contains functionality that is only available during testing.

package etcdq

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

// SetRepollInterval is only used in testing to set the interval to  a very low value.
func (q *Queue[T]) SetRepollInterval(interval time.Duration) {
	q.repollInterval = interval
}

// Clear clears all data in the queue. This is only for testing!
func (q *Queue[T]) Clear() (int64, error) {
	resp, err := q.kv.Delete(context.Background(), q.prefix+"/", clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("clearing queue: %w", err)
	}
	return resp.Deleted, nil
}
