// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package statefun

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewCancellationToken(t *testing.T) {
	_, err := NewCancellationToken("")
	assert.Error(t, err, "empty strings should fail token validation")

	token, err := NewCancellationToken("token")
	assert.NoError(t, err, "failed to validate token")
	assert.Equal(t, "token", token.Token(), "failed to return correct token")
}
