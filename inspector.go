package asynq

// Inspector is used to inspect queues.
type Inspector struct {
	rdb *rdb
}

// NewInspector returns a new Inspector instance.
func NewInspector(opt *RedisOpt) *Inspector {
	return &Inspector{
		rdb: newRDB(opt),
	}
}

// CurrentStats returns a current stats of queues.
func (i *Inspector) CurrentStats() (*Stats, error) {
	return i.rdb.currentStats()
}
