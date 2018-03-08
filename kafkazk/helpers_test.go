package kafkazk

import (
	"testing"
)

func TestWhatChanged(t *testing.T) {
	expected := []string{
		"decreased replication",
		"increased replication",
		"no-op",
		"preferred leader",
		"replaced broker",
		"decreased replication, replaced broker",
		"increased replication, replaced broker",
	}

	inputs := [][2][]int{
		[2][]int{[]int{1000, 1001}, []int{1000}},
		[2][]int{[]int{1000, 1001}, []int{1000, 1001, 1002}},
		[2][]int{[]int{1000, 1001}, []int{1000, 1001}},
		[2][]int{[]int{1000, 1001}, []int{1001, 1000}},
		[2][]int{[]int{1000, 1001}, []int{1000, 1002}},
		[2][]int{[]int{1000, 1001}, []int{1002}},
		[2][]int{[]int{1000, 1001}, []int{1002, 1001, 1003}},
	}

	for i, inputPair := range inputs {
		c := WhatChanged(inputPair[0], inputPair[1])
		if c != expected[i] {
			t.Errorf("Expected change string '%s', got '%s'", expected[i], c)
		}
	}
}
