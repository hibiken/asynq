package broker

import (
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

// This package exports the same types as the internal package.
// This is a temporary solution until we can move the these types out of internal.

type (
	TaskMessage = base.TaskMessage
	WorkerInfo  = base.WorkerInfo
	ServerInfo  = base.ServerInfo

	Broker = base.Broker

	CancellationSubscription = base.CancellationSubscription

	RDB = rdb.RDB
)

var (
	NewRDB = rdb.NewRDB
)
