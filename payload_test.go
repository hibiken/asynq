package asynq

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestPayloadGet(t *testing.T) {
	names := []string{"luke", "anakin", "rey"}
	primes := []int{2, 3, 5, 7, 11, 13, 17}
	user := map[string]interface{}{"name": "Ken", "score": 3.14}
	location := map[string]string{"address": "123 Main St.", "state": "NY", "zipcode": "10002"}
	favs := map[string][]string{
		"movies":   []string{"forrest gump", "star wars"},
		"tv_shows": []string{"game of throwns", "HIMYM", "breaking bad"},
	}
	counter := map[string]int{
		"a": 1,
		"b": 101,
		"c": 42,
	}
	features := map[string]bool{
		"A": false,
		"B": true,
		"C": true,
	}
	now := time.Now()
	duration := 15 * time.Minute

	payload := Payload{
		"greeting":  "Hello",
		"user_id":   9876,
		"pi":        3.1415,
		"enabled":   false,
		"names":     names,
		"primes":    primes,
		"user":      user,
		"location":  location,
		"favs":      favs,
		"counter":   counter,
		"features":  features,
		"timestamp": now,
		"duration":  duration,
	}

	gotStr, err := payload.GetString("greeting")
	if gotStr != "Hello" || err != nil {
		t.Errorf("Payload.GetString(%q) = %v, %v, want %v, nil",
			"greeting", gotStr, err, "Hello")
	}

	gotInt, err := payload.GetInt("user_id")
	if gotInt != 9876 || err != nil {
		t.Errorf("Payload.GetInt(%q) = %v, %v, want, %v, nil",
			"user_id", gotInt, err, 9876)
	}

	gotFloat, err := payload.GetFloat64("pi")
	if gotFloat != 3.1415 || err != nil {
		t.Errorf("Payload.GetFloat64(%q) = %v, %v, want, %v, nil",
			"pi", gotFloat, err, 3.141592)
	}

	gotBool, err := payload.GetBool("enabled")
	if gotBool != false || err != nil {
		t.Errorf("Payload.GetBool(%q) = %v, %v, want, %v, nil",
			"enabled", gotBool, err, false)
	}

	gotStrSlice, err := payload.GetStringSlice("names")
	if diff := cmp.Diff(gotStrSlice, names); diff != "" {
		t.Errorf("Payload.GetStringSlice(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"names", gotStrSlice, err, names, diff)
	}

	gotIntSlice, err := payload.GetIntSlice("primes")
	if diff := cmp.Diff(gotIntSlice, primes); diff != "" {
		t.Errorf("Payload.GetIntSlice(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"primes", gotIntSlice, err, primes, diff)
	}

	gotStrMap, err := payload.GetStringMap("user")
	if diff := cmp.Diff(gotStrMap, user); diff != "" {
		t.Errorf("Payload.GetStringMap(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"user", gotStrMap, err, user, diff)
	}

	gotStrMapStr, err := payload.GetStringMapString("location")
	if diff := cmp.Diff(gotStrMapStr, location); diff != "" {
		t.Errorf("Payload.GetStringMapString(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"location", gotStrMapStr, err, location, diff)
	}

	gotStrMapStrSlice, err := payload.GetStringMapStringSlice("favs")
	if diff := cmp.Diff(gotStrMapStrSlice, favs); diff != "" {
		t.Errorf("Payload.GetStringMapStringSlice(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"favs", gotStrMapStrSlice, err, favs, diff)
	}

	gotStrMapInt, err := payload.GetStringMapInt("counter")
	if diff := cmp.Diff(gotStrMapInt, counter); diff != "" {
		t.Errorf("Payload.GetStringMapInt(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"counter", gotStrMapInt, err, counter, diff)
	}

	gotStrMapBool, err := payload.GetStringMapBool("features")
	if diff := cmp.Diff(gotStrMapBool, features); diff != "" {
		t.Errorf("Payload.GetStringMapBool(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"features", gotStrMapBool, err, features, diff)
	}

	gotTime, err := payload.GetTime("timestamp")
	if !gotTime.Equal(now) {
		t.Errorf("Payload.GetTime(%q) = %v, %v, want %v, nil",
			"timestamp", gotTime, err, now)
	}

	gotDuration, err := payload.GetDuration("duration")
	if gotDuration != duration {
		t.Errorf("Payload.GetDuration(%q) = %v, %v, want %v, nil",
			"duration", gotDuration, err, duration)
	}
}

func TestPayloadGetWithMarshaling(t *testing.T) {
	names := []string{"luke", "anakin", "rey"}
	primes := []int{2, 3, 5, 7, 11, 13, 17}
	user := map[string]interface{}{"name": "Ken", "score": 3.14}
	location := map[string]string{"address": "123 Main St.", "state": "NY", "zipcode": "10002"}
	favs := map[string][]string{
		"movies":   []string{"forrest gump", "star wars"},
		"tv_shows": []string{"game of throwns", "HIMYM", "breaking bad"},
	}
	counter := map[string]int{
		"a": 1,
		"b": 101,
		"c": 42,
	}
	features := map[string]bool{
		"A": false,
		"B": true,
		"C": true,
	}
	now := time.Now()
	duration := 15 * time.Minute

	in := Payload{
		"subject":      "Hello",
		"recipient_id": 9876,
		"pi":           3.14,
		"enabled":      true,
		"names":        names,
		"primes":       primes,
		"user":         user,
		"location":     location,
		"favs":         favs,
		"counter":      counter,
		"features":     features,
		"timestamp":    now,
		"duration":     duration,
	}

	// encode and then decode
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Payload
	err = json.Unmarshal(data, &out)
	if err != nil {
		t.Fatal(err)
	}

	gotStr, err := out.GetString("subject")
	if gotStr != "Hello" || err != nil {
		t.Errorf("Payload.GetString(%q) = %v, %v; want %q, nil",
			"subject", gotStr, err, "Hello")
	}

	gotInt, err := out.GetInt("recipient_id")
	if gotInt != 9876 || err != nil {
		t.Errorf("Payload.GetInt(%q) = %v, %v; want %v, nil",
			"recipient_id", gotInt, err, 9876)
	}

	gotFloat, err := out.GetFloat64("pi")
	if gotFloat != 3.14 || err != nil {
		t.Errorf("Payload.GetFloat64(%q) = %v, %v; want %v, nil",
			"pi", gotFloat, err, 3.14)
	}

	gotBool, err := out.GetBool("enabled")
	if gotBool != true || err != nil {
		t.Errorf("Payload.GetBool(%q) = %v, %v; want %v, nil",
			"enabled", gotBool, err, true)
	}

	gotStrSlice, err := out.GetStringSlice("names")
	if diff := cmp.Diff(gotStrSlice, names); diff != "" {
		t.Errorf("Payload.GetStringSlice(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"names", gotStrSlice, err, names, diff)
	}

	gotIntSlice, err := out.GetIntSlice("primes")
	if diff := cmp.Diff(gotIntSlice, primes); diff != "" {
		t.Errorf("Payload.GetIntSlice(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"primes", gotIntSlice, err, primes, diff)
	}

	gotStrMap, err := out.GetStringMap("user")
	if diff := cmp.Diff(gotStrMap, user); diff != "" {
		t.Errorf("Payload.GetStringMap(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"user", gotStrMap, err, user, diff)
	}

	gotStrMapStr, err := out.GetStringMapString("location")
	if diff := cmp.Diff(gotStrMapStr, location); diff != "" {
		t.Errorf("Payload.GetStringMapString(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"location", gotStrMapStr, err, location, diff)
	}

	gotStrMapStrSlice, err := out.GetStringMapStringSlice("favs")
	if diff := cmp.Diff(gotStrMapStrSlice, favs); diff != "" {
		t.Errorf("Payload.GetStringMapStringSlice(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"favs", gotStrMapStrSlice, err, favs, diff)
	}

	gotStrMapInt, err := out.GetStringMapInt("counter")
	if diff := cmp.Diff(gotStrMapInt, counter); diff != "" {
		t.Errorf("Payload.GetStringMapInt(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"counter", gotStrMapInt, err, counter, diff)
	}

	gotStrMapBool, err := out.GetStringMapBool("features")
	if diff := cmp.Diff(gotStrMapBool, features); diff != "" {
		t.Errorf("Payload.GetStringMapBool(%q) = %v, %v, want %v, nil;\n(-want,+got)\n%s",
			"features", gotStrMapBool, err, features, diff)
	}

	gotTime, err := out.GetTime("timestamp")
	if !gotTime.Equal(now) {
		t.Errorf("Payload.GetTime(%q) = %v, %v, want %v, nil",
			"timestamp", gotTime, err, now)
	}

	gotDuration, err := out.GetDuration("duration")
	if gotDuration != duration {
		t.Errorf("Payload.GetDuration(%q) = %v, %v, want %v, nil",
			"duration", gotDuration, err, duration)
	}
}

func TestPayloadHas(t *testing.T) {
	payload := Payload{
		"user_id": 123,
	}

	if !payload.Has("user_id") {
		t.Errorf("Payload.Has(%q) = false, want true", "user_id")
	}
	if payload.Has("name") {
		t.Errorf("Payload.Has(%q) = true, want false", "name")
	}
}
