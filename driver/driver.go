package driver

import (
	"sync"
)

// constant values
const (
	TagsLabel = `tags`
	VarsLabel = `tmpVars`
)

// Driver represents a data processor.
type Driver interface {
	Process(Payload)
}

// Payload is passed through a Driver and stores and retrieves data as needed to process the desired Result.
type Payload interface {
	Bytes() []byte
	UseBytes([]byte)
	Error() error
	UseError(error)
	KV(id string) KVStore
	Results() chan Result
	Discard()
}

type payload struct {
	b       []byte
	err     error
	lock    sync.Mutex
	kv      map[string]KVStore
	results chan Result
}

// NewPayload returns a new Payload.
func NewPayload() Payload {
	P := payload{
		lock:    sync.Mutex{},
		kv:      make(map[string]KVStore),
		results: make(chan Result),
	}
	P.kv[TagsLabel] = NewKVStore()
	P.kv[VarsLabel] = NewKVStore()
	return &P
}

func (p *payload) Bytes() []byte {
	return p.b
}

func (p *payload) UseBytes(b []byte) {
	p.b = b
}

func (p *payload) Error() error {
	return p.err
}

func (p *payload) UseError(err error) {
	p.err = err
}

func (p *payload) KV(id string) KVStore {
	var kvs KVStore
	p.lock.Lock()
	if _, there := p.kv[id]; !there {
		p.kv[id] = NewKVStore()
	}
	kvs = p.kv[id]
	p.lock.Unlock()
	return kvs
}

func (p *payload) Results() chan Result {
	return p.results
}

func (p *payload) Discard() {
	close(p.results)
}

// Result contains the results of processing data through Drivers and any errors along the way.
type Result interface {
	Bytes() []byte
	Error() error
}

type result struct {
	data []byte
	err  error
}

// NewResult conveniently takes []byte data and an error and creates a Result.
func NewResult(d []byte, err error) Result {
	return &result{
		data: d,
		err:  err,
	}
}

// Bytes returns the underlying data.
func (r *result) Bytes() []byte {
	return r.data
}

// Error returns the underlying error.
func (r *result) Error() error {
	return r.err
}

// KVStore stores key value pairs.
type KVStore interface {
	Get(string) interface{}
	Add(string, interface{})
	Remove(string)
	Use(map[string]interface{})
	All() map[string]interface{}
}

type kvStore struct {
	kv   map[string]interface{}
	lock sync.Mutex
}

// NewKVStore returns a New KVStore.
func NewKVStore() KVStore {
	return &kvStore{
		kv:   make(map[string]interface{}),
		lock: sync.Mutex{},
	}
}

func (kvs *kvStore) Get(k string) interface{} {
	kvs.lock.Lock()
	val := kvs.kv[k]
	kvs.lock.Unlock()
	return val
}

func (kvs *kvStore) Add(k string, v interface{}) {
	kvs.lock.Lock()
	kvs.kv[k] = v
	kvs.lock.Unlock()
}

func (kvs *kvStore) Remove(k string) {
	kvs.lock.Lock()
	delete(kvs.kv, k)
	kvs.lock.Unlock()
}

func (kvs *kvStore) Use(kv map[string]interface{}) {
	kvs.lock.Lock()
	kvs.kv = kv
	kvs.lock.Unlock()
}

func (kvs *kvStore) All() map[string]interface{} {
	tmp := make(map[string]interface{})
	kvs.lock.Lock()
	for k, v := range kvs.kv {
		tmp[k] = v
	}
	kvs.lock.Unlock()
	return tmp
}
