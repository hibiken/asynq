// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"time"

	"github.com/spf13/cast"
)

// Payload is an arbitrary data needed for task execution.
type Payload struct {
	data map[string]interface{}
}

type errKeyNotFound struct {
	key string
}

func (e *errKeyNotFound) Error() string {
	return fmt.Sprintf("key %q does not exist", e.key)
}

// Has reports whether key exists.
func (p Payload) Has(key string) bool {
	_, ok := p.data[key]
	return ok
}

// GetString returns a string value if a string type is associated with
// the key, otherwise reports an error.
func (p Payload) GetString(key string) (string, error) {
	v, ok := p.data[key]
	if !ok {
		return "", &errKeyNotFound{key}
	}
	return cast.ToStringE(v)
}

// GetInt returns an int value if a numeric type is associated with
// the key, otherwise reports an error.
func (p Payload) GetInt(key string) (int, error) {
	v, ok := p.data[key]
	if !ok {
		return 0, &errKeyNotFound{key}
	}
	return cast.ToIntE(v)
}

// GetFloat64 returns a float64 value if a numeric type is associated with
// the key, otherwise reports an error.
func (p Payload) GetFloat64(key string) (float64, error) {
	v, ok := p.data[key]
	if !ok {
		return 0, &errKeyNotFound{key}
	}
	return cast.ToFloat64E(v)
}

// GetBool returns a boolean value if a boolean type is associated with
// the key, otherwise reports an error.
func (p Payload) GetBool(key string) (bool, error) {
	v, ok := p.data[key]
	if !ok {
		return false, &errKeyNotFound{key}
	}
	return cast.ToBoolE(v)
}

// GetStringSlice returns a slice of strings if a string slice type is associated with
// the key, otherwise reports an error.
func (p Payload) GetStringSlice(key string) ([]string, error) {
	v, ok := p.data[key]
	if !ok {
		return nil, &errKeyNotFound{key}
	}
	return cast.ToStringSliceE(v)
}

// GetIntSlice returns a slice of ints if a int slice type is associated with
// the key, otherwise reports an error.
func (p Payload) GetIntSlice(key string) ([]int, error) {
	v, ok := p.data[key]
	if !ok {
		return nil, &errKeyNotFound{key}
	}
	return cast.ToIntSliceE(v)
}

// GetStringMap returns a map of string to empty interface
// if a correct map type is associated with the key,
// otherwise reports an error.
func (p Payload) GetStringMap(key string) (map[string]interface{}, error) {
	v, ok := p.data[key]
	if !ok {
		return nil, &errKeyNotFound{key}
	}
	return cast.ToStringMapE(v)
}

// GetStringMapString returns a map of string to string
// if a correct map type is associated with the key,
// otherwise reports an error.
func (p Payload) GetStringMapString(key string) (map[string]string, error) {
	v, ok := p.data[key]
	if !ok {
		return nil, &errKeyNotFound{key}
	}
	return cast.ToStringMapStringE(v)
}

// GetStringMapStringSlice returns a map of string to string slice
// if a correct map type is associated with the key,
// otherwise reports an error.
func (p Payload) GetStringMapStringSlice(key string) (map[string][]string, error) {
	v, ok := p.data[key]
	if !ok {
		return nil, &errKeyNotFound{key}
	}
	return cast.ToStringMapStringSliceE(v)
}

// GetStringMapInt returns a map of string to int
// if a correct map type is associated with the key,
// otherwise reports an error.
func (p Payload) GetStringMapInt(key string) (map[string]int, error) {
	v, ok := p.data[key]
	if !ok {
		return nil, &errKeyNotFound{key}
	}
	return cast.ToStringMapIntE(v)
}

// GetStringMapBool returns a map of string to boolean
// if a correct map type is associated with the key,
// otherwise reports an error.
func (p Payload) GetStringMapBool(key string) (map[string]bool, error) {
	v, ok := p.data[key]
	if !ok {
		return nil, &errKeyNotFound{key}
	}
	return cast.ToStringMapBoolE(v)
}

// GetTime returns a time value if a correct map type is associated with the key,
// otherwise reports an error.
func (p Payload) GetTime(key string) (time.Time, error) {
	v, ok := p.data[key]
	if !ok {
		return time.Time{}, &errKeyNotFound{key}
	}
	return cast.ToTimeE(v)
}

// GetDuration returns a duration value if a correct map type is associated with the key,
// otherwise reports an error.
func (p Payload) GetDuration(key string) (time.Duration, error) {
	v, ok := p.data[key]
	if !ok {
		return 0, &errKeyNotFound{key}
	}
	return cast.ToDurationE(v)
}
