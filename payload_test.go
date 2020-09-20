// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
)

type payloadTest struct {
	data   map[string]interface{}
	key    string
	nonkey string
}

func TestPayloadString(t *testing.T) {
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"name": "gopher"},
			key:    "name",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetString(tc.key)
		if err != nil || got != tc.data[tc.key] {
			t.Errorf("Payload.GetString(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetString(tc.key)
		if err != nil || got != tc.data[tc.key] {
			t.Errorf("With Marshaling: Payload.GetString(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// access non-existent key.
		got, err = payload.GetString(tc.nonkey)
		if err == nil || got != "" {
			t.Errorf("Payload.GetString(%q) = %v, %v; want '', error",
				tc.key, got, err)
		}
	}
}

func TestPayloadInt(t *testing.T) {
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"user_id": 42},
			key:    "user_id",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetInt(tc.key)
		if err != nil || got != tc.data[tc.key] {
			t.Errorf("Payload.GetInt(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetInt(tc.key)
		if err != nil || got != tc.data[tc.key] {
			t.Errorf("With Marshaling: Payload.GetInt(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// access non-existent key.
		got, err = payload.GetInt(tc.nonkey)
		if err == nil || got != 0 {
			t.Errorf("Payload.GetInt(%q) = %v, %v; want 0, error",
				tc.key, got, err)
		}
	}
}

func TestPayloadFloat64(t *testing.T) {
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"pi": 3.14},
			key:    "pi",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetFloat64(tc.key)
		if err != nil || got != tc.data[tc.key] {
			t.Errorf("Payload.GetFloat64(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetFloat64(tc.key)
		if err != nil || got != tc.data[tc.key] {
			t.Errorf("With Marshaling: Payload.GetFloat64(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// access non-existent key.
		got, err = payload.GetFloat64(tc.nonkey)
		if err == nil || got != 0 {
			t.Errorf("Payload.GetFloat64(%q) = %v, %v; want 0, error",
				tc.key, got, err)
		}
	}
}

func TestPayloadBool(t *testing.T) {
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"enabled": true},
			key:    "enabled",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetBool(tc.key)
		if err != nil || got != tc.data[tc.key] {
			t.Errorf("Payload.GetBool(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetBool(tc.key)
		if err != nil || got != tc.data[tc.key] {
			t.Errorf("With Marshaling: Payload.GetBool(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// access non-existent key.
		got, err = payload.GetBool(tc.nonkey)
		if err == nil || got != false {
			t.Errorf("Payload.GetBool(%q) = %v, %v; want false, error",
				tc.key, got, err)
		}
	}
}

func TestPayloadStringSlice(t *testing.T) {
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"names": []string{"luke", "rey", "anakin"}},
			key:    "names",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetStringSlice(tc.key)
		diff := cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("Payload.GetStringSlice(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetStringSlice(tc.key)
		diff = cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("With Marshaling: Payload.GetStringSlice(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// access non-existent key.
		got, err = payload.GetStringSlice(tc.nonkey)
		if err == nil || got != nil {
			t.Errorf("Payload.GetStringSlice(%q) = %v, %v; want nil, error",
				tc.key, got, err)
		}
	}
}

func TestPayloadIntSlice(t *testing.T) {
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"nums": []int{9, 8, 7}},
			key:    "nums",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetIntSlice(tc.key)
		diff := cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("Payload.GetIntSlice(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetIntSlice(tc.key)
		diff = cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("With Marshaling: Payload.GetIntSlice(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// access non-existent key.
		got, err = payload.GetIntSlice(tc.nonkey)
		if err == nil || got != nil {
			t.Errorf("Payload.GetIntSlice(%q) = %v, %v; want nil, error",
				tc.key, got, err)
		}
	}
}

func TestPayloadStringMap(t *testing.T) {
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"user": map[string]interface{}{"name": "Jon Doe", "score": 2.2}},
			key:    "user",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetStringMap(tc.key)
		diff := cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("Payload.GetStringMap(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetStringMap(tc.key)
		ignoreOpt := cmpopts.IgnoreMapEntries(func(key string, val interface{}) bool {
			switch val.(type) {
			case json.Number:
				return true
			default:
				return false
			}
		})
		diff = cmp.Diff(got, tc.data[tc.key], ignoreOpt)
		if err != nil || diff != "" {
			t.Errorf("With Marshaling: Payload.GetStringMap(%q) = %v, %v, want %v, nil;(-want,+got)\n%s",
				tc.key, got, err, tc.data[tc.key], diff)
		}

		// access non-existent key.
		got, err = payload.GetStringMap(tc.nonkey)
		if err == nil || got != nil {
			t.Errorf("Payload.GetStringMap(%q) = %v, %v; want nil, error",
				tc.key, got, err)
		}
	}
}

func TestPayloadStringMapString(t *testing.T) {
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"address": map[string]string{"line": "123 Main St", "city": "San Francisco", "state": "CA"}},
			key:    "address",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetStringMapString(tc.key)
		diff := cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("Payload.GetStringMapString(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetStringMapString(tc.key)
		diff = cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("With Marshaling: Payload.GetStringMapString(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// access non-existent key.
		got, err = payload.GetStringMapString(tc.nonkey)
		if err == nil || got != nil {
			t.Errorf("Payload.GetStringMapString(%q) = %v, %v; want nil, error",
				tc.key, got, err)
		}
	}
}

func TestPayloadStringMapStringSlice(t *testing.T) {
	favs := map[string][]string{
		"movies":   {"forrest gump", "star wars"},
		"tv_shows": {"game of thrones", "HIMYM", "breaking bad"},
	}
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"favorites": favs},
			key:    "favorites",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetStringMapStringSlice(tc.key)
		diff := cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("Payload.GetStringMapStringSlice(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetStringMapStringSlice(tc.key)
		diff = cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("With Marshaling: Payload.GetStringMapStringSlice(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// access non-existent key.
		got, err = payload.GetStringMapStringSlice(tc.nonkey)
		if err == nil || got != nil {
			t.Errorf("Payload.GetStringMapStringSlice(%q) = %v, %v; want nil, error",
				tc.key, got, err)
		}
	}
}

func TestPayloadStringMapInt(t *testing.T) {
	counter := map[string]int{
		"a": 1,
		"b": 101,
		"c": 42,
	}
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"counts": counter},
			key:    "counts",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetStringMapInt(tc.key)
		diff := cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("Payload.GetStringMapInt(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetStringMapInt(tc.key)
		diff = cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("With Marshaling: Payload.GetStringMapInt(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// access non-existent key.
		got, err = payload.GetStringMapInt(tc.nonkey)
		if err == nil || got != nil {
			t.Errorf("Payload.GetStringMapInt(%q) = %v, %v; want nil, error",
				tc.key, got, err)
		}
	}
}

func TestPayloadStringMapBool(t *testing.T) {
	features := map[string]bool{
		"A": false,
		"B": true,
		"C": true,
	}
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"features": features},
			key:    "features",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetStringMapBool(tc.key)
		diff := cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("Payload.GetStringMapBool(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetStringMapBool(tc.key)
		diff = cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("With Marshaling: Payload.GetStringMapBool(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// access non-existent key.
		got, err = payload.GetStringMapBool(tc.nonkey)
		if err == nil || got != nil {
			t.Errorf("Payload.GetStringMapBool(%q) = %v, %v; want nil, error",
				tc.key, got, err)
		}
	}
}

func TestPayloadTime(t *testing.T) {
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"current": time.Now()},
			key:    "current",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetTime(tc.key)
		diff := cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("Payload.GetTime(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetTime(tc.key)
		diff = cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("With Marshaling: Payload.GetTime(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// access non-existent key.
		got, err = payload.GetTime(tc.nonkey)
		if err == nil || !got.IsZero() {
			t.Errorf("Payload.GetTime(%q) = %v, %v; want %v, error",
				tc.key, got, err, time.Time{})
		}
	}
}

func TestPayloadDuration(t *testing.T) {
	tests := []payloadTest{
		{
			data:   map[string]interface{}{"duration": 15 * time.Minute},
			key:    "duration",
			nonkey: "unknown",
		},
	}

	for _, tc := range tests {
		payload := Payload{tc.data}

		got, err := payload.GetDuration(tc.key)
		diff := cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("Payload.GetDuration(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// encode and then decode task messsage.
		in := h.NewTaskMessage("testing", tc.data)
		encoded, err := base.EncodeMessage(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := base.DecodeMessage(encoded)
		if err != nil {
			t.Fatal(err)
		}
		payload = Payload{out.Payload}
		got, err = payload.GetDuration(tc.key)
		diff = cmp.Diff(got, tc.data[tc.key])
		if err != nil || diff != "" {
			t.Errorf("With Marshaling: Payload.GetDuration(%q) = %v, %v, want %v, nil",
				tc.key, got, err, tc.data[tc.key])
		}

		// access non-existent key.
		got, err = payload.GetDuration(tc.nonkey)
		if err == nil || got != 0 {
			t.Errorf("Payload.GetDuration(%q) = %v, %v; want %v, error",
				tc.key, got, err, time.Duration(0))
		}
	}
}

func TestPayloadHas(t *testing.T) {
	payload := Payload{map[string]interface{}{
		"user_id": 123,
	}}

	if !payload.Has("user_id") {
		t.Errorf("Payload.Has(%q) = false, want true", "user_id")
	}
	if payload.Has("name") {
		t.Errorf("Payload.Has(%q) = true, want false", "name")
	}
}

func TestPayloadDebuggingStrings(t *testing.T) {
	data := map[string]interface{}{
		"foo": 123,
		"bar": "hello",
		"baz": false,
	}
	payload := Payload{data: data}

	if payload.String() != fmt.Sprint(data) {
		t.Errorf("Payload.String() = %q, want %q",
			payload.String(), fmt.Sprint(data))
	}

	got, err := payload.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("Payload.MarhsalJSON() = %s, want %s; (-want,+got)\n%s",
			got, want, diff)
	}
}
