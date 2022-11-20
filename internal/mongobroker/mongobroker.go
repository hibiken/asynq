package mongobroker

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/timeutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// LeaseDuration is the duration used to initially create a lease and to extend it thereafter.
const LeaseDuration = 30 * time.Second

type Task struct {
	ID           string    `bson:"_id" json:"_id"`
	Msg          []byte    `bson:"msg" json:"msg"`
	State        string    `bson:"state" json:"state"`
	PendingSince int64     `bson:"pending_since" json:"pending_since,omitempty"`
	LeasedUntil  int64     `bson:"leased_until" json:"leased_until,omitempty"`
	CompletedAt  int64     `bson:"completed_at" json:"completed_at,omitempty"`
	ExpiresAt    time.Time `bson:"expires_at,omitempty" json:"expires_at,omitempty"`
	ProcessAt    time.Time `bson:"process_at,omitempty" json:"process_at,omitempty"`
}

type MongoBroker struct {
	client *mongo.Client
	db     *mongo.Database
	ctx    context.Context
	clock  timeutil.Clock
}

func NewBroker(c *mongo.Client, db string, dbopts *options.DatabaseOptions, ctx context.Context) *MongoBroker {
	if db == "" {
		db = "asynq"
	}
	mgb := &MongoBroker{
		client: c,
		ctx:    ctx,
		db:     c.Database(db, dbopts),
		clock:  timeutil.NewRealClock(),
	}
	mgb.CreateQueue("servers")
	return mgb
}

func (m *MongoBroker) Ping() error {
	return m.client.Ping(m.ctx, readpref.PrimaryPreferred())
}
func (m *MongoBroker) Close() error {
	return m.client.Disconnect(m.ctx)
}

func (m *MongoBroker) CreateQueue(name string) error {
	if cols, err := m.db.ListCollectionNames(m.ctx, bson.M{"name": name}); err != nil {
		return err
	} else {
		for _, col := range cols {
			if col == name {
				return nil
			}
		}

		// could not find collection, let's add it
		colerr := m.db.CreateCollection(m.ctx, name, &options.CreateCollectionOptions{})
		if colerr != nil {
			return colerr
		}
		// expireAfterSeconds will delete all docs that have expires_at set
		// Mongo will not delete docs that don't have a value in expires_at
		// Mongo will expire after 0 seconds from the value written in expires_at
		m.db.Collection(name).Indexes().CreateOne(m.ctx, mongo.IndexModel{
			Keys:    bson.D{{"expires_at", 1}},
			Options: options.Index().SetExpireAfterSeconds(1),
		})
	}

	return nil
}

func (m *MongoBroker) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "mongo.Enqueue"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := m.CreateQueue(msg.Queue); err != nil {
		return errors.E(op, errors.NotFound, err)
	}

	if sr := m.db.Collection(msg.Queue).FindOne(m.ctx, bson.M{"_id": msg.ID}); sr.Err() == nil {
		// we have found a doc
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}

	task := &Task{
		ID:           msg.ID,
		Msg:          encoded,
		State:        "pending",
		PendingSince: m.clock.Now().UnixNano(),
	}
	_, err = m.db.Collection(msg.Queue).InsertOne(m.ctx, task)

	if err != nil {
		return errors.E(op, errors.Internal, err)
	}

	return nil
}

func (m *MongoBroker) getUniqueColl() *mongo.Collection {
	return m.db.Collection("asynq__unique_keys")
}

func (m *MongoBroker) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, ttl time.Duration) error {
	var op errors.Op = "mongo.EnqueueUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := m.CreateQueue(msg.Queue); err != nil {
		return errors.E(op, errors.NotFound, err)
	}

	ur := m.getUniqueColl().FindOne(m.ctx, bson.M{"_id": msg.UniqueKey, "expires_at": bson.M{"$gt": m.clock.Now()}})
	if ur.Err() == nil {
		// we found the unique id, can't do it again
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}

	if sr := m.db.Collection(msg.Queue).FindOne(m.ctx, bson.M{"_id": msg.ID}); sr.Err() == nil {
		// we have found a doc with the same ID on the queue
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}

	task := &Task{
		ID:           msg.ID,
		Msg:          encoded,
		State:        "pending",
		PendingSince: m.clock.Now().UnixNano(),
	}
	_, err = m.db.Collection(msg.Queue).InsertOne(m.ctx, task)

	if err != nil {
		return errors.E(op, errors.Internal, err)
	}

	_, err = m.getUniqueColl().InsertOne(m.ctx, bson.M{"_id": msg.UniqueKey, "expires_at": m.clock.Now().Add(ttl)})
	if err != nil {
		return errors.E(op, errors.Internal, err)
	}

	return nil
}

func (m *MongoBroker) Dequeue(qnames ...string) (*base.TaskMessage, time.Time, error) {
	var op errors.Op = "mongo.Dequeue"
	for _, qname := range qnames {
		leaseExpirationTime := m.clock.Now().Add(LeaseDuration)
		res := m.db.Collection(qname).FindOneAndUpdate(m.ctx, bson.M{
			"state": "pending",
		},
			bson.M{"$set": bson.M{"state": "active", "leased_until": leaseExpirationTime, "pending_since": nil}},
		)
		if res.Err() == mongo.ErrNoDocuments {
			continue
		} else if res.Err() != nil {
			// let's break on a Mongo error
			return nil, time.Time{}, errors.E(op, errors.Internal, res.Err())
		}

		var task = &Task{}
		if err := res.Decode(task); err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, res.Err())
		}
		if msg, err := base.DecodeMessage(task.Msg); err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
		} else {
			return msg, leaseExpirationTime, nil
		}
	}
	return nil, time.Time{}, errors.E(op, errors.NotFound, errors.ErrNoProcessableTask)
}

// Done will update stats and delete the task. Used when Retention is set to 0
func (m *MongoBroker) Done(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "mongo.Done"
	res := m.db.Collection(msg.Queue).FindOneAndDelete(m.ctx, bson.M{"_id": msg.ID})
	if res.Err() == mongo.ErrNoDocuments {
		return nil
	} else if res.Err() != nil {
		return errors.E(op, res.Err())
	}
	return nil
}

// MarkAsComplete will update stats and mark the task as completed. Used with Retention
func (m *MongoBroker) MarkAsComplete(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "mongo.MarkAsComplete"

	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}

	res := m.db.Collection(msg.Queue).FindOneAndReplace(m.ctx, bson.M{"_id": msg.ID},
		&Task{
			ID:          msg.ID,
			State:       "completed",
			CompletedAt: m.clock.Now().Unix(),
			ExpiresAt:   m.clock.Now().Add(time.Second * time.Duration(msg.Retention)),
			Msg:         encoded,
		},
	)
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return errors.E(op, errors.NotFound)
		}
		return errors.E(op, res.Err())
	}
	return nil
}
func (m *MongoBroker) Requeue(ctx context.Context, msg *base.TaskMessage) error {
	fmt.Println("Requeue")
	return nil
}
func (m *MongoBroker) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	var op errors.Op = "mongo.Schedule"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := m.CreateQueue(msg.Queue); err != nil {
		return errors.E(op, errors.NotFound, err)
	}

	if sr := m.db.Collection(msg.Queue).FindOne(m.ctx, bson.M{"_id": msg.ID}); sr.Err() == nil {
		// we have found a doc
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}

	task := &Task{
		ID:        msg.ID,
		Msg:       encoded,
		State:     "scheduled",
		ProcessAt: processAt,
	}
	_, err = m.db.Collection(msg.Queue).InsertOne(m.ctx, task)

	if err != nil {
		return errors.E(op, errors.Internal, err)
	}

	return nil
}

// ScheduleUnique adds the task to the backlog queue to be processed in the future if the uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (m *MongoBroker) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	var op errors.Op = "mongo.ScheduleUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := m.CreateQueue(msg.Queue); err != nil {
		return errors.E(op, errors.NotFound, err)
	}

	ur := m.getUniqueColl().FindOne(m.ctx, bson.M{"_id": msg.UniqueKey, "expires_at": bson.M{"$gt": m.clock.Now()}})
	if ur.Err() == nil {
		// we found the unique id, can't do it again
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}

	if sr := m.db.Collection(msg.Queue).FindOne(m.ctx, bson.M{"_id": msg.ID}); sr.Err() == nil {
		// we have found a doc
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}

	task := &Task{
		ID:        msg.ID,
		Msg:       encoded,
		State:     "scheduled",
		ProcessAt: processAt,
	}
	_, err = m.db.Collection(msg.Queue).InsertOne(m.ctx, task)

	if err != nil {
		return errors.E(op, errors.Internal, err)
	}

	return nil
}

// Retry moves the task from active to retry queue.
// It also annotates the message with the given error message and
// if isFailure is true increments the retried counter.
func (m *MongoBroker) Retry(ctx context.Context, msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	var op errors.Op = "mongo.Retry"
	now := m.clock.Now()
	modified := *msg
	if isFailure {
		modified.Retried++
	}
	modified.ErrorMsg = errMsg
	modified.LastFailedAt = now.Unix()
	encoded, err := base.EncodeMessage(&modified)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}

	task := &Task{
		ID:        msg.ID,
		Msg:       encoded,
		State:     "retry",
		ProcessAt: processAt,
	}
	sr := m.db.Collection(msg.Queue).FindOneAndReplace(m.ctx, bson.D{{"_id", msg.ID}}, task)
	if sr.Err() == mongo.ErrNoDocuments {
		return errors.E(op, errors.TaskNotFoundError{Queue: msg.Queue, ID: msg.ID})
	} else if sr.Err() != nil {
		return errors.E(op, errors.Internal, err)
	}

	return nil
}

// Archive sends the given task to archive, attaching the error message to the task.
func (m *MongoBroker) Archive(ctx context.Context, msg *base.TaskMessage, errMsg string) error {
	var op errors.Op = "mongo.Archive"
	now := m.clock.Now()
	modified := *msg
	modified.ErrorMsg = errMsg
	modified.LastFailedAt = now.Unix()
	encoded, err := base.EncodeMessage(&modified)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}

	task := &Task{
		ID:    msg.ID,
		Msg:   encoded,
		State: "archived",
	}
	sr := m.db.Collection(msg.Queue).FindOneAndReplace(m.ctx, bson.D{{"_id", msg.ID}}, task)
	if sr.Err() == mongo.ErrNoDocuments {
		return errors.E(op, errors.TaskNotFoundError{Queue: msg.Queue, ID: msg.ID})
	} else if sr.Err() != nil {
		return errors.E(op, errors.Internal, err)
	}

	return nil
}

// ForwardIfReady checks scheduled and retry sets of the given queues
// and move any tasks that are ready to be processed to the pending set.
func (m *MongoBroker) ForwardIfReady(qnames ...string) error {
	var op errors.Op = "mongo.ForwardIfReady"
	for _, qname := range qnames {
		if err := m.forwardAll(qname); err != nil {
			return errors.E(op, errors.CanonicalCode(err), err)
		}
	}
	return nil
}

// forwardAll forwards all tasks that are ready to be processed to "pending"
func (m *MongoBroker) forwardAll(qname string) error {
	_, err := m.db.Collection(qname).UpdateMany(
		m.ctx,
		bson.M{"state": bson.M{"$in": bson.A{"retry", "scheduled"}}, "process_at": bson.M{"$lte": m.clock.Now()}},
		bson.D{{"$set", bson.M{"state": "pending"}}},
	)
	if err != nil {
		return errors.E(errors.Internal, err)
	}
	return nil
}

// Group aggregation related methods
func (m *MongoBroker) AddToGroup(ctx context.Context, msg *base.TaskMessage, gname string) error {
	fmt.Println("AddToGroup")
	return nil
}
func (m *MongoBroker) AddToGroupUnique(ctx context.Context, msg *base.TaskMessage, groupKey string, ttl time.Duration) error {
	fmt.Println("AddToGroupUnique")
	return nil
}
func (m *MongoBroker) ListGroups(qname string) ([]string, error) {
	fmt.Println("ListGroups")
	return nil, nil
}
func (m *MongoBroker) AggregationCheck(qname, gname string, t time.Time, gracePeriod, maxDelay time.Duration, maxSize int) (aggregationSetID string, err error) {
	fmt.Println("AggregationCheck")
	return "", nil
}
func (m *MongoBroker) ReadAggregationSet(qname, gname, aggregationSetID string) ([]*base.TaskMessage, time.Time, error) {
	fmt.Println("ReadAggregationSet")
	return nil, time.Now(), nil
}
func (m *MongoBroker) DeleteAggregationSet(ctx context.Context, qname, gname, aggregationSetID string) error {
	fmt.Println("DeleteAggregationSet")
	return nil
}
func (m *MongoBroker) ReclaimStaleAggregationSets(qname string) error {
	fmt.Println("ReclaimStaleAggregationSets")
	return nil
}

// Task retention related method
func (m *MongoBroker) DeleteExpiredCompletedTasks(qname string) error {
	//fmt.Println("DeleteExpiredCompletedTasks")
	return nil
}

// Lease related methods
func (m *MongoBroker) ListLeaseExpired(cutoff time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	fmt.Println("ListLeaseExpired")
	return nil, nil
}
func (m *MongoBroker) ExtendLease(qname string, ids ...string) (time.Time, error) {
	fmt.Println("ExtendLease")
	return time.Now(), nil
}

// State snapshot related methods
func (m *MongoBroker) WriteServerState(info *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	type WorkerInfo struct {
		ID       string
		Type     string
		Payload  []byte
		Queue    string
		Started  time.Time
		Deadline time.Time
	}

	type ServerInfo struct {
		Server    *base.ServerInfo   `bson:"server,inline,omitempty"`
		Workers   []*base.WorkerInfo `bson:"workers,omitempty"`
		ExpiresAt time.Time          `bson:"expires_at,omitempty"`
	}

	var workersDb []WorkerInfo
	for _, w := range workers {
		workerDb := WorkerInfo{
			ID:       w.ID,
			Type:     w.Type,
			Payload:  w.Payload,
			Queue:    w.Queue,
			Started:  w.Started,
			Deadline: w.Deadline,
		}
		workersDb = append(workersDb, workerDb)
	}

	_, err := m.db.Collection("servers").ReplaceOne(
		m.ctx,
		bson.M{"host": info.Host, "pid": info.PID, "serverid": info.ServerID},
		ServerInfo{
			Server:    info,
			Workers:   workers,
			ExpiresAt: m.clock.Now().Add(ttl),
		},
		options.Replace().SetUpsert(true),
	)
	if err != nil {
		return errors.E(errors.Internal, err)
	}
	return nil
}
func (m *MongoBroker) ClearServerState(host string, pid int, serverID string) error {
	m.db.Collection("servers").FindOneAndDelete(
		m.ctx,
		bson.M{"host": host, "pid": pid, "server_id": serverID},
	)
	return nil
}

// Cancelation related methods
func (m *MongoBroker) CancelationPubSub() (*redis.PubSub, error) { // TODO: Need to decouple from redis to support other brokers
	fmt.Println("CancelationPubSub")
	return nil, nil
}
func (m *MongoBroker) PublishCancelation(id string) error {
	fmt.Println("PublishCancelation")
	return nil
}

func (m *MongoBroker) WriteResult(qname, id string, data []byte) (n int, err error) {
	fmt.Println("WriteResult")
	return 0, nil
}
