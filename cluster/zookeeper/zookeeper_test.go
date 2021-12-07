package zookeeper

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIdFromZnode(t *testing.T) {
	type test struct {
		input  string
		result int
		err    error
	}

	tests := map[int]test{
		1: {
			input:  "/locks/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001",
			result: 1,
			err:    nil,
		},
		2: {
			input:  "/locks/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000002",
			result: 2,
			err:    nil,
		},
		3: {
			input:  "/locks/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lo",
			result: 0,
			err:    ErrInvalidSeqNode,
		},
	}

	for tcase, test := range tests {
		result, err := idFromZnode(test.input)
		if test.err == nil {
			assert.Nil(t, err)
		}
		assert.Equal(t, result, test.result, fmt.Sprintf("case %d", tcase))
	}
}
