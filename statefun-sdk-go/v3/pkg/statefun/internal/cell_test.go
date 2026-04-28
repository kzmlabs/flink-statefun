// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal/protocol"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestNewCell(t *testing.T) {
	cell := NewCell(&protocol.ToFunction_PersistedValue{
		StateName: "state",
	}, "typename")

	assert.False(t, cell.HasValue(), "cell should not have a value")
	assert.Equal(t, cell.typeTypeName, "typename")

	cell = NewCell(&protocol.ToFunction_PersistedValue{
		StateName: "state",
		StateValue: &protocol.TypedValue{
			Typename: "blah",
			HasValue: false,
			Value:    []byte{1, 1, 1, 1},
		},
	}, "typename")

	assert.False(t, cell.HasValue(), "cell should not have a value")
	assert.Equal(t, cell.typeTypeName, "typename")

	cell = NewCell(&protocol.ToFunction_PersistedValue{
		StateName: "state",
		StateValue: &protocol.TypedValue{
			Typename: "blah",
			HasValue: true,
			Value:    []byte{1, 1, 1, 1},
		},
	}, "typename")

	assert.True(t, cell.HasValue(), "cell should have a value")
	assert.Equal(t, cell.typeTypeName, "typename")
}

func TestCellReadWrite(t *testing.T) {
	cell := NewCell(&protocol.ToFunction_PersistedValue{
		StateName: "state",
	}, "typename")

	data := []byte{0, 1, 0, 1}
	n, err := cell.Write(data)

	assert.NoError(t, err, "unexpected error writing data")
	assert.Equal(t, len(data), n, "unexpected number of bytes written")

	read := make([]byte, 4)

	cell.SeekToBeginning()
	n, err = cell.Read(read)

	assert.NoError(t, err, "unexpected error reading data")
	assert.Equal(t, len(read), n, "unexpected number of bytes read")

	assert.Equal(t, data, read, "unexpected bytes read from cell")

	cell.SeekToBeginning()
	allBytes, _ := io.ReadAll(cell)

	assert.Equal(t, len(allBytes), n, "unexpected number of bytes read")
	assert.Equal(t, data, allBytes, "unexpected bytes read from cell")

	secondRead := make([]byte, 4)

	cell.SeekToBeginning()
	n, err = cell.Read(secondRead)

	assert.NoError(t, err, "unexpected error reading data")
	assert.Equal(t, len(secondRead), n, "unexpected number of bytes read")

	assert.Equal(t, data, secondRead, "unexpected bytes read from cell")

	cell.SeekToBeginning()
	allBytes, _ = io.ReadAll(cell)

	assert.Equal(t, len(allBytes), n, "unexpected number of bytes read")
	assert.Equal(t, data, allBytes, "unexpected bytes read from cell")
}

func TestCellReadWriteFromInitial(t *testing.T) {
	initial := []byte{0, 1, 0, 1}
	cell := NewCell(&protocol.ToFunction_PersistedValue{
		StateName: "state",
		StateValue: &protocol.TypedValue{
			Typename: "typename",
			HasValue: true,
			Value:    initial,
		},
	}, "typename")

	read := make([]byte, 4)
	n, err := cell.Read(read)

	assert.NoError(t, err, "unexpected error reading data")
	assert.Equal(t, len(read), n, "unexpected number of bytes read")

	assert.Equal(t, initial, read, "unexpected bytes read from cell")

	data := []byte{0, 0, 0, 1, 1, 1}
	n, err = cell.Write(data)

	assert.NoError(t, err, "unexpected error writing data")
	assert.Equal(t, len(data), n, "unexpected number of bytes written")

	secondRead := make([]byte, 6)
	n, err = cell.Read(secondRead)

	assert.NoError(t, err, "unexpected error reading data")
	assert.Equal(t, len(secondRead), n, "unexpected number of bytes read")

	assert.Equal(t, data, secondRead, "unexpected bytes read from cell")
}

func TestCell_EmptyWithNoValue(t *testing.T) {
	initial := []byte{0, 1, 0, 1}
	cell := NewCell(&protocol.ToFunction_PersistedValue{
		StateName: "state",
		StateValue: &protocol.TypedValue{
			Typename: "typename",
			HasValue: true,
			Value:    initial,
		},
	}, "typename")

	assert.True(t, cell.HasValue())

	cell.Delete()
	assert.False(t, cell.HasValue(), "cells that have been deleted should not have values")
}

func TestCell_ChunkedWrites(t *testing.T) {
	cell := NewCell(&protocol.ToFunction_PersistedValue{
		StateName: "state",
	}, "typename")

	data := []byte{0, 1, 0, 1}
	n, err := cell.Write(data)
	assert.NoError(t, err, "unexpected error writing data")
	assert.Equal(t, len(data), n, "unexpected number of bytes written")

	n, err = cell.Write(data)
	assert.NoError(t, err, "unexpected error writing data")
	assert.Equal(t, len(data), n, "unexpected number of bytes written")

	assert.Equal(t, len(data)*2, len(cell.buf), "unexpected number of bytes")
	assert.Equal(t, []byte{0, 1, 0, 1, 0, 1, 0, 1}, cell.buf, "unexpected results written to cell")
}
