// Package linearizability records histories of key-value operations and
// checks them with Porcupine (github.com/anishathalye/porcupine).
//
// A history is linearizable if every operation can be placed at a single
// instant between its call and its return such that the resulting
// sequence is legal for a key-value map. Writes whose outcome the client
// never learned (a timeout, a crash) are recorded as returning at the end
// of time, so the checker may place them anywhere after their call,
// including after every read, which is the same as never happening.
package linearizability

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// Kind is the kind of operation.
type Kind byte

const (
	Get Kind = iota
	Put
	Delete
)

// Input is an operation as the client issued it.
type Input struct {
	Kind  Kind
	Key   string
	Value string // for Put
}

// Output is what the client observed.
type Output struct {
	Found   bool   // for Get
	Value   string // for Get
	Unknown bool   // a write whose outcome is unknown
}

type register struct {
	exists bool
	value  string
}

// Model is a map from keys to values, checked one key at a time.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make(map[string][]porcupine.Operation)
		for _, op := range history {
			k := op.Input.(Input).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return register{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		r, in, out := state.(register), input.(Input), output.(Output)
		switch in.Kind {
		case Get:
			return out.Found == r.exists && out.Value == r.value, r
		case Put:
			return true, register{exists: true, value: in.Value}
		case Delete:
			return true, register{}
		}
		return false, r
	},
	Equal: func(a, b interface{}) bool { return a.(register) == b.(register) },
	DescribeOperation: func(input, output interface{}) string {
		in, out := input.(Input), output.(Output)
		switch {
		case in.Kind == Get && out.Found:
			return fmt.Sprintf("get(%s) -> %q", in.Key, out.Value)
		case in.Kind == Get:
			return fmt.Sprintf("get(%s) -> not found", in.Key)
		case in.Kind == Put && out.Unknown:
			return fmt.Sprintf("put(%s, %q) -> unknown", in.Key, in.Value)
		case in.Kind == Put:
			return fmt.Sprintf("put(%s, %q)", in.Key, in.Value)
		case out.Unknown:
			return fmt.Sprintf("delete(%s) -> unknown", in.Key)
		default:
			return fmt.Sprintf("delete(%s)", in.Key)
		}
	},
}

// Recorder collects a history from concurrent clients.
type Recorder struct {
	start time.Time
	mu    sync.Mutex
	ops   []porcupine.Operation
}

// NewRecorder starts a history. Timestamps are nanoseconds since this call.
func NewRecorder() *Recorder { return &Recorder{start: time.Now()} }

// Now returns the current timestamp. It uses the monotonic clock.
func (r *Recorder) Now() int64 { return time.Since(r.start).Nanoseconds() }

// Record adds an operation that completed between call and ret.
func (r *Recorder) Record(client int, in Input, out Output, call, ret int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret})
}

// RecordUnknown adds a write that was issued at call but whose outcome the
// client never learned.
func (r *Recorder) RecordUnknown(client int, in Input, call int64) {
	r.Record(client, in, Output{Unknown: true}, call, math.MaxInt64)
}

// Len returns the number of recorded operations.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}

// Check runs Porcupine on the history. It returns porcupine.Unknown if the
// check does not finish within timeout.
func (r *Recorder) Check(timeout time.Duration) (porcupine.CheckResult, porcupine.LinearizationInfo) {
	r.mu.Lock()
	ops := append([]porcupine.Operation(nil), r.ops...)
	r.mu.Unlock()
	return porcupine.CheckOperationsVerbose(Model, ops, timeout)
}
