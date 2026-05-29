package kafkazk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReassignments(t *testing.T) {
	type testCase struct {
		input    TopicMetadata
		expected Reassignments
	}

	tests := []testCase{
		// Case 1: we have both adding and removing.
		{
			input: TopicMetadata{
				Version:          3,
				Name:             "test",
				TopicID:          "foobar",
				Partitions:       map[int][]int{0: {0, 1, 2}},
				AddingReplicas:   map[int][]int{0: {0}},
				RemovingReplicas: map[int][]int{0: {2}},
			},
			expected: Reassignments{
				"test": {0: []int{0, 1}},
			},
		},
		// Case 2: we have only adding.
		{
			input: TopicMetadata{
				Version:          3,
				Name:             "test",
				TopicID:          "foobar",
				Partitions:       map[int][]int{0: {0, 1}},
				AddingReplicas:   map[int][]int{0: {0}},
				RemovingReplicas: map[int][]int{},
			},
			expected: Reassignments{
				"test": {0: []int{0, 1}},
			},
		},
		// Case 3: we have only removing.
		{
			input: TopicMetadata{
				Version:          3,
				Name:             "test",
				TopicID:          "foobar",
				Partitions:       map[int][]int{0: {0, 1, 2}},
				AddingReplicas:   map[int][]int{},
				RemovingReplicas: map[int][]int{0: {2}},
			},
			expected: Reassignments{
				"test": {0: []int{0, 1}},
			},
		},
		// Case 4: we have multiple partitions, one with removing and one with adding.
		{
			input: TopicMetadata{
				Version: 3,
				Name:    "test",
				TopicID: "foobar",
				Partitions: map[int][]int{
					0: {0, 1, 2, 5},
					1: {2, 3, 4, 5},
				},
				AddingReplicas: map[int][]int{
					0: {0},
				},
				RemovingReplicas: map[int][]int{
					0: {5},
					1: {4},
				},
			},
			expected: Reassignments{
				"test": {
					0: []int{0, 1, 2},
					1: []int{2, 3, 5}},
			},
		},
		// Case 5: we have neither adding/removing.
		{
			input: TopicMetadata{
				Version:          3,
				Name:             "test",
				TopicID:          "foobar",
				Partitions:       map[int][]int{0: {0, 1}},
				AddingReplicas:   map[int][]int{},
				RemovingReplicas: map[int][]int{},
			},
			expected: Reassignments{
				"test": map[int][]int{},
			},
		},
	}

	for _, test := range tests {
		result := test.input.Reassignments()
		assert.Equal(t, test.expected, result, "unexpected reassignment data")
	}
}
