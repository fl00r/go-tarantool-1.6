package crud

import (
	"context"

	"github.com/tarantool/go-tarantool"
)

// CountResult describes result for `crud.count` method.
type CountResult = NumberResult

// CountOpts describes options for `crud.count` method.
type CountOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout OptUint
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter OptString
	// Mode is a parameter with `write`/`read` possible values,
	// if `write` is specified then operation is performed on master.
	Mode OptString
	// PreferReplica is a parameter to specify preferred target
	// as one of the replicas.
	PreferReplica OptBool
	// Balance is a parameter to use replica according to vshard
	// load balancing policy.
	Balance OptBool
	// YieldEvery describes number of tuples processed to yield after.
	YieldEvery OptUint
	// BucketId is a bucket ID.
	BucketId OptUint
	// ForceMapCall describes the map call is performed without any
	// optimizations even if full primary key equal condition is specified.
	ForceMapCall OptBool
	// Fullscan describes if a critical log entry will be skipped on
	// potentially long count.
	Fullscan OptBool
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts CountOpts) EncodeMsgpack(enc *encoder) error {
	const optsCnt = 9

	options := [optsCnt]option{opts.Timeout, opts.VshardRouter,
		opts.Mode, opts.PreferReplica, opts.Balance,
		opts.YieldEvery, opts.BucketId, opts.ForceMapCall,
		opts.Fullscan}
	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		modeOptName, preferReplicaOptName, balanceOptName,
		yieldEveryOptName, bucketIdOptName, forceMapCallOptName,
		fullscanOptName}
	values := [optsCnt]interface{}{}

	return encodeOptions(enc, options[:], names[:], values[:])
}

// CountRequest helps you to create request object to call `crud.count`
// for execution by a Connection.
type CountRequest struct {
	spaceRequest
	conditions []Condition
	opts       CountOpts
}

type countArgs struct {
	_msgpack   struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space      string
	Conditions []Condition
	Opts       CountOpts
}

// NewCountRequest returns a new empty CountRequest.
func NewCountRequest(space string) *CountRequest {
	req := new(CountRequest)
	req.initImpl("crud.count")
	req.setSpace(space)
	req.conditions = nil
	req.opts = CountOpts{}
	return req
}

// Conditions sets the conditions for the CountRequest request.
// Note: default value is nil.
func (req *CountRequest) Conditions(conditions []Condition) *CountRequest {
	req.conditions = conditions
	return req
}

// Opts sets the options for the CountRequest request.
// Note: default value is nil.
func (req *CountRequest) Opts(opts CountOpts) *CountRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req *CountRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := countArgs{Space: req.space, Conditions: req.conditions, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req *CountRequest) Context(ctx context.Context) *CountRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
