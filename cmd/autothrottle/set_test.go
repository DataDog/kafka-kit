package main

import (
	"sort"
	"testing"
)

func TestAddHas(t *testing.T) {
	s := newSet()
	s.add("key")

	if !s.has("key") {
		t.Error("Expected set to have key 'key'")
	}

	if s.has("nil") {
		t.Error("Unexpected key in set")
	}
}

func TestCopy(t *testing.T) {
	s := newSet()
	s.add("key")

	s2 := s.copy()

	if !s2.has("key") {
		t.Error("Expected set to have key 'key'")
	}
}

func TestKeys(t *testing.T) {
	expected := []string{"key1", "key2", "key3"}

	s := newSet()
	for _, key := range expected {
		s.add(key)
	}

	sort.Strings(expected)
	got := s.keys()
	sort.Strings(got)

	if len(got) != len(expected) {
		t.Fatalf("Expected keys len %d, got %d", len(expected), len(got))
	}

	for i, key := range expected {
		if got[i] != key {
			t.Errorf("Expected set to have key '%s'", key)
		}
	}
}

func TestDiff(t *testing.T) {
	s1 := newSet()
	s2 := newSet()

	for _, key := range []string{"key1", "key2", "key3"} {
		s1.add(key)
		s2.add(key)
	}

	s2.add("key4")
	diff := s2.diff(s1)

	if len(diff) != 1 && !diff.has("key4") {
		t.Errorf("Expected diff keys %v, got %v\n", []string{"key4"}, diff.keys())
	}
}

func TestIsSubSet(t *testing.T) {
	s1 := newSet()
	s2 := newSet()

	for _, key := range []string{"key1", "key2", "key3"} {
		s1.add(key)
		s2.add(key)
	}

	s2.add("key4")

	if !s1.isSubSet(s2) {
		t.Errorf("Expected %v to be subset of %v\n", s1.keys(), s2.keys())
	}

	if s2.isSubSet(s1) {
		t.Errorf("Expected %v to not be subset of %v\n", s1.keys(), s2.keys())
	}
}
