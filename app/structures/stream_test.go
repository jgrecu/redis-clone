package structures

import (
	"reflect"
	"strings"
	"testing"
)

func TestNewStream(t *testing.T) {
	s := NewStream()
	if s == nil {
		t.Fatal("NewStream() returned nil")
	}
	if s.Len() != 0 {
		t.Errorf("NewStream().Len() = %d, want 0", s.Len())
	}
	if s.LastTimestamp() != -1 {
		t.Errorf("NewStream().LastTimestamp() = %d, want -1", s.LastTimestamp())
	}
}

func TestStream_Add_ExplicitID(t *testing.T) {
	s := NewStream()

	key, err := s.Add("1-1", map[string]string{"field": "value"})
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if key != "1-1" {
		t.Errorf("Add() key = %s, want 1-1", key)
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d, want 1", s.Len())
	}
}

func TestStream_Add_AutoSequence(t *testing.T) {
	s := NewStream()

	s.Add("1-1", map[string]string{"a": "1"})
	key, err := s.Add("1-*", map[string]string{"b": "2"})
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if key != "1-2" {
		t.Errorf("Add() auto-seq key = %s, want 1-2", key)
	}
}

func TestStream_Add_AutoTimestamp(t *testing.T) {
	s := NewStream()

	key, err := s.Add("*", map[string]string{"field": "value"})
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if !strings.Contains(key, "-") {
		t.Errorf("Add(*) key = %s, expected timestamp-seq format", key)
	}
}

func TestStream_Add_ZeroZeroRejected(t *testing.T) {
	s := NewStream()

	_, err := s.Add("0-0", map[string]string{"a": "b"})
	if err == nil {
		t.Error("Add(0-0) should return error")
	}
	if !strings.Contains(err.Error(), "greater than 0-0") {
		t.Errorf("Add(0-0) error = %v, want 'greater than 0-0'", err)
	}
}

func TestStream_Add_NegativeTimestampRejected(t *testing.T) {
	s := NewStream()

	_, err := s.Add("-5-1", nil)
	if err == nil {
		t.Error("Add(-5-1) should return error")
	}
}

func TestStream_Add_DuplicateIDRejected(t *testing.T) {
	s := NewStream()

	s.Add("1-1", map[string]string{"a": "1"})
	_, err := s.Add("1-1", map[string]string{"b": "2"})
	if err == nil {
		t.Error("Add() with duplicate ID should return error")
	}
	if !strings.Contains(err.Error(), "equal or smaller") {
		t.Errorf("Add() error = %v, want 'equal or smaller'", err)
	}
}

func TestStream_Add_SmallerIDRejected(t *testing.T) {
	s := NewStream()

	s.Add("5-1", map[string]string{"a": "1"})
	_, err := s.Add("3-1", map[string]string{"b": "2"})
	if err == nil {
		t.Error("Add() with smaller ID should return error")
	}
}

func TestStream_Add_SameTimestampSmallerSeqRejected(t *testing.T) {
	s := NewStream()

	s.Add("5-5", map[string]string{"a": "1"})
	_, err := s.Add("5-3", map[string]string{"b": "2"})
	if err == nil {
		t.Error("Add() with same timestamp but smaller seq should return error")
	}
}

func TestStream_Add_EmptyPairs(t *testing.T) {
	s := NewStream()

	key, err := s.Add("1-1", map[string]string{})
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if key != "1-1" {
		t.Errorf("Add() = %s, want 1-1", key)
	}
}

func TestStream_Add_MultiplePairs(t *testing.T) {
	s := NewStream()

	pairs := map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"}
	key, err := s.Add("1-1", pairs)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	got, ok := s.Get(key)
	if !ok {
		t.Fatal("Get() returned false for just-added key")
	}
	if !reflect.DeepEqual(got, pairs) {
		t.Errorf("Get() pairs = %v, want %v", got, pairs)
	}
}

func TestStream_Get_NonExistent(t *testing.T) {
	s := NewStream()

	_, ok := s.Get("1-1")
	if ok {
		t.Error("Get() on empty stream should return false")
	}
}

func TestStream_Get_InvalidKey(t *testing.T) {
	s := NewStream()

	_, ok := s.Get("invalid")
	if ok {
		t.Error("Get() with invalid key should return false")
	}
}

func TestStream_AutoSequence_ZeroTimestamp(t *testing.T) {
	s := NewStream()

	key, err := s.Add("0-*", map[string]string{"a": "b"})
	if err != nil {
		t.Fatalf("Add(0-*) error = %v", err)
	}
	if key != "0-1" {
		t.Errorf("Add(0-*) = %s, want 0-1", key)
	}
}

func TestStream_Read(t *testing.T) {
	s := NewStream()
	s.Add("1-1", map[string]string{"a": "1"})
	s.Add("1-2", map[string]string{"b": "2"})
	s.Add("2-1", map[string]string{"c": "3"})

	entries := s.Read("1-1")
	if len(entries) != 2 {
		t.Errorf("Read(1-1) returned %d entries, want 2", len(entries))
	}
}

func TestStream_Read_EmptyStream(t *testing.T) {
	s := NewStream()

	entries := s.Read("0-0")
	if len(entries) != 0 {
		t.Errorf("Read() on empty stream returned %d entries, want 0", len(entries))
	}
}

func TestStream_Read_NothingAfterLast(t *testing.T) {
	s := NewStream()
	s.Add("1-1", map[string]string{"a": "1"})

	entries := s.Read("1-1")
	if len(entries) != 0 {
		t.Errorf("Read() after last entry returned %d entries, want 0", len(entries))
	}
}

func TestStream_Len(t *testing.T) {
	s := NewStream()
	if s.Len() != 0 {
		t.Errorf("Empty stream Len() = %d, want 0", s.Len())
	}

	s.Add("1-1", map[string]string{"a": "1"})
	if s.Len() != 1 {
		t.Errorf("After 1 add, Len() = %d, want 1", s.Len())
	}

	s.Add("2-1", map[string]string{"b": "2"})
	s.Add("3-1", map[string]string{"c": "3"})
	if s.Len() != 3 {
		t.Errorf("After 3 adds, Len() = %d, want 3", s.Len())
	}
}

func TestStream_LastSeq(t *testing.T) {
	s := NewStream()

	if s.LastSeq(1) != -1 {
		t.Errorf("LastSeq on empty = %d, want -1", s.LastSeq(1))
	}

	s.Add("1-5", map[string]string{"a": "1"})
	if s.LastSeq(1) != 5 {
		t.Errorf("LastSeq(1) = %d, want 5", s.LastSeq(1))
	}

	s.Add("1-10", map[string]string{"b": "2"})
	if s.LastSeq(1) != 10 {
		t.Errorf("LastSeq(1) after second add = %d, want 10", s.LastSeq(1))
	}
}

func TestStream_LastTimestamp(t *testing.T) {
	s := NewStream()
	if s.LastTimestamp() != -1 {
		t.Errorf("Empty stream LastTimestamp() = %d, want -1", s.LastTimestamp())
	}

	s.Add("5-1", map[string]string{"a": "1"})
	if s.LastTimestamp() != 5 {
		t.Errorf("LastTimestamp() = %d, want 5", s.LastTimestamp())
	}

	s.Add("10-1", map[string]string{"b": "2"})
	if s.LastTimestamp() != 10 {
		t.Errorf("LastTimestamp() = %d, want 10", s.LastTimestamp())
	}
}

func TestParseKey(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		wantTimestamp int64
		wantSeq       string
		wantErr       bool
	}{
		{"Wildcard", "*", -1, "*", false},
		{"Normal ID", "123-456", 123, "456", false},
		{"Zero ID", "0-0", 0, "0", false},
		{"Missing seq", "123", 0, "0", false},
		{"Non-numeric timestamp", "abc-1", 0, "", true},
		{"Non-numeric seq (valid parse, error later)", "1-abc", 1, "abc", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, seq, err := parseKey(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseKey(%q) error = %v, wantErr %v", tt.key, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if ts != tt.wantTimestamp {
					t.Errorf("parseKey(%q) timestamp = %d, want %d", tt.key, ts, tt.wantTimestamp)
				}
				if seq != tt.wantSeq {
					t.Errorf("parseKey(%q) seq = %s, want %s", tt.key, seq, tt.wantSeq)
				}
			}
		})
	}
}

func TestEntry(t *testing.T) {
	pairs := map[string]string{"f1": "v1"}
	e := NewEntry(100, 5, pairs)

	if e.Timestamp() != 100 {
		t.Errorf("Timestamp() = %d, want 100", e.Timestamp())
	}
	if e.Seq() != 5 {
		t.Errorf("Seq() = %d, want 5", e.Seq())
	}
	if e.Key() != "100-5" {
		t.Errorf("Key() = %s, want 100-5", e.Key())
	}
	if !reflect.DeepEqual(e.Pairs, pairs) {
		t.Errorf("Pairs = %v, want %v", e.Pairs, pairs)
	}
}

func TestStream_Range_NoDuplicates(t *testing.T) {
	s := NewStream()
	s.Add("1-1", map[string]string{"a": "1"})
	s.Add("2-1", map[string]string{"b": "2"})
	s.Add("3-1", map[string]string{"c": "3"})

	entries := s.Range("1-1", "3-1")
	if len(entries) != 3 {
		t.Errorf("Range returned %d entries, want 3 (no duplicates)", len(entries))
	}

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

func TestStream_Range_StartEqualsEnd(t *testing.T) {
	s := NewStream()
	s.Add("1-1", map[string]string{"a": "1"})

	entries := s.Range("1-1", "1-1")
	if len(entries) != 1 {
		t.Errorf("Range(1-1, 1-1) returned %d entries, want 1", len(entries))
	}
}

func TestStream_Range_Empty(t *testing.T) {
	s := NewStream()

	entries := s.Range("0-0", "1-0")
	if len(entries) != 0 {
		t.Errorf("Range on empty stream returned %d entries, want 0", len(entries))
	}
}

func TestStream_Range_SameTimestamp(t *testing.T) {
	s := NewStream()
	s.Add("1-1", map[string]string{"a": "1"})
	s.Add("1-2", map[string]string{"b": "2"})
	s.Add("1-3", map[string]string{"c": "3"})

	entries := s.Range("1-1", "1-3")
	if len(entries) != 3 {
		t.Errorf("Range same timestamp returned %d entries, want 3", len(entries))
	}
}
