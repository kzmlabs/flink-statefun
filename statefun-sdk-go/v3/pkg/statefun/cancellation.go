// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package statefun

import (
	"errors"
	"fmt"
)

// CancellationToken tags a delayed message send with statefun.SendAfterWithCancellationToken.
// It can then be used to cancel said message on a best effort basis with statefun.CancelDelayedMessage.
// The underlying string token can be retrieved by invoking Token().
type CancellationToken interface {
	fmt.Stringer

	// Token returns the underlying string
	// used to create the CancellationToken.
	Token() string

	// prevents external implementations
	// of the interface.
	internal()
}

type token string

func (t token) String() string {
	return "CancellationToken(" + string(t) + ")"
}

func (t token) Token() string {
	return string(t)
}

func (t token) internal() {}

// NewCancellationToken creates a new cancellation token or
// returns an error if the token is invalid.
func NewCancellationToken(t string) (CancellationToken, error) {
	if len(t) == 0 {
		return nil, errors.New("cancellation token cannot be empty")
	}
	return token(t), nil
}
