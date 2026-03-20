package structures

import (
	"reflect"
	"testing"
	"time"
)

func TestStore_Get(t *testing.T) {
	s := NewStore()
	s.Set("key1", "value1", time.Time{})

	val, ok := s.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("Get(key1) = (%q, %v), want (\"value1\", true)", val, ok)
	}
}

func TestStore_Get_NonExistent(t *testing.T) {
	s := NewStore()

	_, ok := s.Get("missing")
	if ok {
		t.Error("Get(missing) should return false")
	}
}

func TestStore_Get_EmptyValue(t *testing.T) {
	s := NewStore()
	s.Set("empty", "", time.Time{})

	val, ok := s.Get("empty")
	if !ok || val != "" {
		t.Errorf("Get(empty) = (%q, %v), want (\"\", true)", val, ok)
	}
}

func TestStore_Get_EmptyKey(t *testing.T) {
	s := NewStore()
	s.Set("", "val", time.Time{})

	val, ok := s.Get("")
	if !ok || val != "val" {
		t.Errorf("Get(\"\") = (%q, %v), want (\"val\", true)", val, ok)
	}
}

func TestStore_Get_Expired(t *testing.T) {
	s := NewStore()
	s.Set("exp", "gone", time.Now().Add(-1*time.Second))

	_, ok := s.Get("exp")
	if ok {
		t.Error("Get(exp) should return false for expired key")
	}
}

func TestStore_Get_NotYetExpired(t *testing.T) {
	s := NewStore()
	s.Set("future", "still here", time.Now().Add(24*time.Hour))

	val, ok := s.Get("future")
	if !ok || val != "still here" {
		t.Errorf("Get(future) = (%q, %v), want (\"still here\", true)", val, ok)
	}
}

func TestStore_Get_ExpiredKeyIsDeleted(t *testing.T) {
	s := NewStore()
	s.Set("exp", "val", time.Now().Add(-1*time.Millisecond))

	s.Get("exp") // triggers lazy delete

	// Verify it's gone even from Type
	if s.Type("exp") != "none" {
		t.Error("Expired key should be deleted from store after Get")
	}
}

func TestStore_Get_NoExpiryNeverExpires(t *testing.T) {
	s := NewStore()
	s.Set("perm", "forever", time.Time{})

	val, ok := s.Get("perm")
	if !ok || val != "forever" {
		t.Errorf("Get(perm) = (%q, %v), want (\"forever\", true)", val, ok)
	}
}

func TestStore_Set_Overwrite(t *testing.T) {
	s := NewStore()
	s.Set("key", "first", time.Time{})
	s.Set("key", "second", time.Time{})

	val, ok := s.Get("key")
	if !ok || val != "second" {
		t.Errorf("Get after overwrite = (%q, %v), want (\"second\", true)", val, ok)
	}
}

func TestStore_Delete(t *testing.T) {
	s := NewStore()
	s.Set("key", "val", time.Time{})
	s.Delete("key")

	_, ok := s.Get("key")
	if ok {
		t.Error("Get after Delete should return false")
	}
}

func TestStore_Delete_NonExistent(t *testing.T) {
	s := NewStore()
	s.Delete("missing") // should not panic
}

func TestStore_Keys(t *testing.T) {
	s := NewStore()

	keys := s.Keys()
	if len(keys) != 0 {
		t.Errorf("Empty store Keys() = %v, want empty", keys)
	}

	s.Set("a", "1", time.Time{})
	s.Set("b", "2", time.Time{})
	keys = s.Keys()
	if len(keys) != 2 {
		t.Errorf("Keys() returned %d keys, want 2", len(keys))
	}
}

func TestStore_Type(t *testing.T) {
	s := NewStore()

	if s.Type("missing") != "none" {
		t.Error("Type(missing) should be 'none'")
	}

	s.Set("str", "val", time.Time{})
	if s.Type("str") != "string" {
		t.Errorf("Type(str) = %q, want 'string'", s.Type("str"))
	}

	s.XAdd("mystream", "1-1", map[string]string{"a": "b"})
	if s.Type("mystream") != "stream" {
		t.Errorf("Type(mystream) = %q, want 'stream'", s.Type("mystream"))
	}
}

func TestStore_Incr_NewKey(t *testing.T) {
	s := NewStore()

	val, err := s.Incr("counter")
	if err != nil || val != 1 {
		t.Errorf("Incr(new) = (%d, %v), want (1, nil)", val, err)
	}
}

func TestStore_Incr_ExistingNumeric(t *testing.T) {
	s := NewStore()
	s.Set("counter", "10", time.Time{})

	val, err := s.Incr("counter")
	if err != nil || val != 11 {
		t.Errorf("Incr(10) = (%d, %v), want (11, nil)", val, err)
	}
}

func TestStore_Incr_NonNumeric(t *testing.T) {
	s := NewStore()
	s.Set("str", "hello", time.Time{})

	_, err := s.Incr("str")
	if err == nil {
		t.Error("Incr(non-numeric) should return error")
	}
}

func TestStore_Incr_Negative(t *testing.T) {
	s := NewStore()
	s.Set("neg", "-5", time.Time{})

	val, err := s.Incr("neg")
	if err != nil || val != -4 {
		t.Errorf("Incr(-5) = (%d, %v), want (-4, nil)", val, err)
	}
}

func TestStore_Incr_Zero(t *testing.T) {
	s := NewStore()
	s.Set("zero", "0", time.Time{})

	val, err := s.Incr("zero")
	if err != nil || val != 1 {
		t.Errorf("Incr(0) = (%d, %v), want (1, nil)", val, err)
	}
}

func TestStore_Incr_Float(t *testing.T) {
	s := NewStore()
	s.Set("f", "3.14", time.Time{})

	_, err := s.Incr("f")
	if err == nil {
		t.Error("Incr(float) should return error")
	}
}

func TestStore_Incr_EmptyString(t *testing.T) {
	s := NewStore()
	s.Set("e", "", time.Time{})

	_, err := s.Incr("e")
	if err == nil {
		t.Error("Incr(empty) should return error")
	}
}

func TestStore_Incr_Twice(t *testing.T) {
	s := NewStore()
	s.Set("c", "5", time.Time{})

	s.Incr("c")
	val, err := s.Incr("c")
	if err != nil || val != 7 {
		t.Errorf("Incr twice from 5 = (%d, %v), want (7, nil)", val, err)
	}
}

func TestStore_LoadKeys(t *testing.T) {
	s := NewStore()
	s.Set("old", "data", time.Time{})

	newDB := make(RedisDB)
	newDB["new1"] = MapValue{Typ: "string", String: "v1"}
	newDB["new2"] = MapValue{Typ: "string", String: "v2"}

	s.LoadKeys(newDB)

	if s.Type("old") != "none" {
		t.Error("LoadKeys should replace old data")
	}
	v1, ok := s.Get("new1")
	if !ok || v1 != "v1" {
		t.Error("LoadKeys did not load new1")
	}
	v2, ok := s.Get("new2")
	if !ok || v2 != "v2" {
		t.Error("LoadKeys did not load new2")
	}
}

func TestStore_XAdd_NewStream(t *testing.T) {
	s := NewStore()

	key, err := s.XAdd("stream1", "1-1", map[string]string{"f": "v"})
	if err != nil || key != "1-1" {
		t.Errorf("XAdd = (%q, %v), want (\"1-1\", nil)", key, err)
	}
	if s.Type("stream1") != "stream" {
		t.Error("XAdd should create a stream type")
	}
}

func TestStore_XAdd_InvalidID(t *testing.T) {
	s := NewStore()

	_, err := s.XAdd("stream1", "0-0", map[string]string{"f": "v"})
	if err == nil {
		t.Error("XAdd(0-0) should return error")
	}
}

func TestStore_XRange(t *testing.T) {
	s := NewStore()
	s.XAdd("stream", "1-1", map[string]string{"a": "1"})
	s.XAdd("stream", "2-1", map[string]string{"b": "2"})

	entries, ok := s.XRange("stream", "0-0", "3-0")
	if !ok {
		t.Fatal("XRange should return true for existing stream")
	}
	if len(entries) != 2 {
		t.Errorf("XRange returned %d entries, want 2", len(entries))
	}
}

func TestStore_XRange_NonExistent(t *testing.T) {
	s := NewStore()

	_, ok := s.XRange("missing", "0-0", "1-0")
	if ok {
		t.Error("XRange on missing stream should return false")
	}
}

func TestStore_XRead(t *testing.T) {
	s := NewStore()
	s.XAdd("stream", "1-1", map[string]string{"a": "1"})
	s.XAdd("stream", "1-2", map[string]string{"b": "2"})

	result := s.XRead([]string{"stream"}, []string{"1-1"})
	entries, ok := result["stream"]
	if !ok {
		t.Fatal("XRead should return entries for existing stream")
	}
	if len(entries) != 1 {
		t.Errorf("XRead returned %d entries, want 1", len(entries))
	}
}

func TestStore_StreamSize(t *testing.T) {
	s := NewStore()

	if s.StreamSize([]string{"missing"}) != 0 {
		t.Error("StreamSize of missing stream should be 0")
	}

	s.XAdd("s1", "1-1", map[string]string{"a": "1"})
	s.XAdd("s1", "2-1", map[string]string{"b": "2"})

	if s.StreamSize([]string{"s1"}) != 2 {
		t.Errorf("StreamSize = %d, want 2", s.StreamSize([]string{"s1"}))
	}
}

func TestStore_LastStreamID(t *testing.T) {
	s := NewStore()

	if s.LastStreamID("missing") != "0-0" {
		t.Error("LastStreamID of missing stream should be '0-0'")
	}

	s.XAdd("s1", "5-3", map[string]string{"a": "1"})
	id := s.LastStreamID("s1")
	if id != "5-3" {
		t.Errorf("LastStreamID = %q, want '5-3'", id)
	}
}

func TestStore_Expiry(t *testing.T) {
	s := NewStore()
	s.Set("exp", "val", time.Now().Add(50*time.Millisecond))

	val, ok := s.Get("exp")
	if !ok || val != "val" {
		t.Error("Key should exist before expiry")
	}

	time.Sleep(100 * time.Millisecond)

	_, ok = s.Get("exp")
	if ok {
		t.Error("Key should be expired after waiting")
	}
}

func TestStore_SetThenGet(t *testing.T) {
	s := NewStore()
	s.Set("k", "v", time.Time{})

	val, ok := s.Get("k")
	if !ok || val != "v" {
		t.Errorf("Set then Get = (%q, %v), want (\"v\", true)", val, ok)
	}
}

func TestStore_SetOverwritesDifferentType(t *testing.T) {
	s := NewStore()
	s.XAdd("key", "1-1", map[string]string{"a": "b"})

	if s.Type("key") != "stream" {
		t.Fatal("setup: key should be stream type")
	}

	s.Set("key", "now_string", time.Time{})
	if s.Type("key") != "string" {
		t.Errorf("Type after Set = %q, want 'string'", s.Type("key"))
	}
	val, ok := s.Get("key")
	if !ok || val != "now_string" {
		t.Errorf("Get after type change = (%q, %v), want (\"now_string\", true)", val, ok)
	}
}

func TestStore_XRange_SingleTimestamp(t *testing.T) {
	s := NewStore()
	s.XAdd("stream", "1-1", map[string]string{"a": "1"})
	s.XAdd("stream", "1-2", map[string]string{"b": "2"})
	s.XAdd("stream", "1-3", map[string]string{"c": "3"})

	entries, ok := s.XRange("stream", "1-1", "1-3")
	if !ok {
		t.Fatal("XRange should return true")
	}
	if len(entries) != 3 {
		t.Errorf("XRange returned %d entries, want 3", len(entries))
	}
}

func TestStore_XRange_NoDuplicates(t *testing.T) {
	s := NewStore()
	s.XAdd("stream", "1-1", map[string]string{"a": "1"})
	s.XAdd("stream", "2-1", map[string]string{"b": "2"})
	s.XAdd("stream", "3-1", map[string]string{"c": "3"})

	entries, ok := s.XRange("stream", "1-1", "3-1")
	if !ok {
		t.Fatal("XRange should return true")
	}
	// This test verifies the Range() bug fix — entries should NOT be duplicated
	if len(entries) != 3 {
		t.Errorf("XRange returned %d entries, want 3 (no duplicates)", len(entries))
	}

	// Verify each entry appears exactly once
	seen := map[string]int{}
	for _, e := range entries {
		seen[e.Key()]++
	}
	for key, count := range seen {
		if count != 1 {
			t.Errorf("Entry %s appears %d times, want 1", key, count)
		}
	}
}

func TestStore_XRead_MultipleStreams(t *testing.T) {
	s := NewStore()
	s.XAdd("s1", "1-1", map[string]string{"a": "1"})
	s.XAdd("s2", "1-1", map[string]string{"b": "2"})

	result := s.XRead([]string{"s1", "s2"}, []string{"0-0", "0-0"})
	if len(result) != 2 {
		t.Errorf("XRead returned %d streams, want 2", len(result))
	}
	if _, ok := result["s1"]; !ok {
		t.Error("XRead should include s1")
	}
	if _, ok := result["s2"]; !ok {
		t.Error("XRead should include s2")
	}
}

func TestStore_XRead_EmptyResult(t *testing.T) {
	s := NewStore()
	s.XAdd("stream", "1-1", map[string]string{"a": "1"})

	// Read after the only entry — nothing new
	result := s.XRead([]string{"stream"}, []string{"1-1"})
	if _, ok := result["stream"]; ok {
		t.Error("XRead should not return entries when nothing is after the given ID")
	}
}

func TestStore_Type_ExpiredKey(t *testing.T) {
	s := NewStore()
	s.Set("exp", "val", time.Now().Add(-1*time.Second))

	// Type does NOT do lazy expiry — this is a known behavior
	// The key is still in the store until a Get triggers cleanup
	typ := s.Type("exp")
	if typ != "string" {
		t.Errorf("Type(expired key before Get) = %q, want 'string'", typ)
	}
}

func TestStore_XRange_NonStreamType(t *testing.T) {
	s := NewStore()
	s.Set("str", "val", time.Time{})

	_, ok := s.XRange("str", "0-0", "1-0")
	if ok {
		t.Error("XRange on string type should return false")
	}
}

func TestStore_Keys_Content(t *testing.T) {
	s := NewStore()
	s.Set("alpha", "1", time.Time{})
	s.Set("beta", "2", time.Time{})

	keys := s.Keys()
	keySet := map[string]bool{}
	for _, k := range keys {
		keySet[k] = true
	}

	if !keySet["alpha"] || !keySet["beta"] {
		t.Errorf("Keys() = %v, want [alpha, beta]", keys)
	}
}

func TestStore_StreamSize_NonStream(t *testing.T) {
	s := NewStore()
	s.Set("str", "val", time.Time{})

	if s.StreamSize([]string{"str"}) != 0 {
		t.Error("StreamSize on string type should be 0")
	}
}

func TestStore_LastStreamID_NonStream(t *testing.T) {
	s := NewStore()
	s.Set("str", "val", time.Time{})

	if s.LastStreamID("str") != "0-0" {
		t.Error("LastStreamID on string type should be '0-0'")
	}
}

func TestStore_XAdd_MultipleEntries(t *testing.T) {
	s := NewStore()
	s.XAdd("stream", "1-1", map[string]string{"a": "1"})
	s.XAdd("stream", "1-2", map[string]string{"b": "2"})
	s.XAdd("stream", "2-1", map[string]string{"c": "3"})

	if s.StreamSize([]string{"stream"}) != 3 {
		t.Errorf("StreamSize = %d, want 3", s.StreamSize([]string{"stream"}))
	}

	// XRead from the beginning
	result := s.XRead([]string{"stream"}, []string{"0-0"})
	if len(result["stream"]) != 3 {
		t.Errorf("XRead all = %d entries, want 3", len(result["stream"]))
	}
}

func TestStore_XRange_BugFix_StartEqualsEnd(t *testing.T) {
	s := NewStore()
	s.XAdd("stream", "1-1", map[string]string{"a": "1"})

	entries, ok := s.XRange("stream", "1-1", "1-1")
	if !ok {
		t.Fatal("XRange should return true")
	}
	// Range with start==end should return exactly that one entry
	if len(entries) != 1 {
		t.Errorf("XRange(1-1, 1-1) returned %d entries, want 1", len(entries))
	}
}

func TestStore_XRange_CrossTimestamp(t *testing.T) {
	s := NewStore()
	s.XAdd("stream", "1-1", map[string]string{"a": "1"})
	s.XAdd("stream", "2-1", map[string]string{"b": "2"})
	s.XAdd("stream", "3-1", map[string]string{"c": "3"})

	// Range spanning multiple timestamps
	entries, ok := s.XRange("stream", "1-0", "3-2")
	if !ok {
		t.Fatal("XRange should return true")
	}
	entryKeys := []string{}
	for _, e := range entries {
		entryKeys = append(entryKeys, e.Key())
	}
	if !reflect.DeepEqual(len(entries), 3) {
		t.Errorf("XRange cross-timestamp = %v (len %d), want 3 entries", entryKeys, len(entries))
	}
}
