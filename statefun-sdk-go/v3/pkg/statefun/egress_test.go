// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

package statefun

import (
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal/protocol"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestKafkaEgressBuilder(t *testing.T) {
	k := KafkaEgressBuilder{
		Target: TypeNameFrom("example/target"),
		Topic:  "topic",
		Key:    "key",
		Value:  "value",
	}

	msg, err := k.toEgressMessage()
	assert.NoError(t, err, "failed to build Kafka egress message")

	var result protocol.KafkaProducerRecord
	err = proto.Unmarshal(msg.Argument.Value, &result)

	assert.NoError(t, err, "failed to deserialize Kafka producer record")
	assert.Equal(t, "key", result.Key)
	assert.Equal(t, "value", string(result.ValueBytes))
	assert.Equal(t, "topic", result.Topic)
}

func TestKafkaEgressBuilderInvalidString(t *testing.T) {
	k := KafkaEgressBuilder{
		Target: TypeNameFrom("example/target"),
		Topic:  "topic",
		Key:    "key",
		Value:  string([]byte{0xff, 0xfe, 0xfd}),
	}

	_, err := k.toEgressMessage()
	assert.Errorf(t, err, "built Kafka egress message with invalid string")
}

func TestKinesisEgressBuilder(t *testing.T) {
	k := KinesisEgressBuilder{
		Target:       TypeNameFrom("example/target"),
		Stream:       "stream",
		PartitionKey: "key",
		Value:        "value",
	}

	msg, err := k.toEgressMessage()
	assert.NoError(t, err, "failed to build Kinesis egress message")

	var result protocol.KinesisEgressRecord
	err = proto.Unmarshal(msg.Argument.Value, &result)

	assert.NoError(t, err, "failed to deserialize Kinesis producer record")
	assert.Equal(t, "stream", result.Stream)
	assert.Equal(t, "key", result.PartitionKey)
	assert.Equal(t, "value", string(result.ValueBytes))
}

func TestKinesisEgressBuilderInvalidString(t *testing.T) {
	k := KinesisEgressBuilder{
		Target:       TypeNameFrom("example/target"),
		Stream:       "stream",
		PartitionKey: "key",
		Value:        string([]byte{0xff, 0xfe, 0xfd}),
	}

	_, err := k.toEgressMessage()
	assert.Errorf(t, err, "built Kinesis egress message with invalid string")
}
