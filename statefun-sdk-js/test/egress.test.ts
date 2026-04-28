// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
'use strict';

import {describe, expect} from '@jest/globals'
import {StateFun} from "../src/statefun";

import {kinesisEgressMessage, kafkaEgressMessage} from "../src/egress";
import {trySerializerForEgress} from "../src/egress";

describe('Egress', () => {

    it('Kafka egress builder should construct a correct record', () => {
        let egress = kafkaEgressMessage({
            typename: "foo/bar",
            topic: "greets",
            value: "hello world"
        });

        expect(egress.typename).toStrictEqual("foo/bar");
        expect(egress.typedValue.getTypename()).toStrictEqual("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord");
        expect(egress.typedValue.getHasValue()).toStrictEqual(true);
        expect(egress.typedValue.getValue_asU8().length > 0).toStrictEqual(true);
    });

    it('Kinesis egress builder should construct a correct record', () => {
        let egress = kinesisEgressMessage({
            typename: "foo/bar",
            stream: "greets",
            partitionKey: "foo",
            value: JSON.stringify({a: "a", b: "b"}),
            valueType: StateFun.jsonType("foo/bar")
        });

        expect(egress.typename).toStrictEqual("foo/bar");
        expect(egress.typedValue.getTypename()).toStrictEqual("type.googleapis.com/io.statefun.sdk.egress.KinesisEgressRecord");
        expect(egress.typedValue.getHasValue()).toStrictEqual(true);
        expect(egress.typedValue.getValue_asU8().length > 0).toStrictEqual(true);
    });

    it('Should deduce a UTF8 string type', () => {
        let bytes = trySerializerForEgress(null, "hello world");
        expect(bytes instanceof Uint8Array).toStrictEqual(true);
        expect(bytes.toString()).toStrictEqual("hello world");
    });

    it('Should accept raw bytes', () => {
        let bytes = trySerializerForEgress(null, Buffer.from("hello world"));
        expect(bytes instanceof Uint8Array).toStrictEqual(true);
        expect(bytes.toString()).toStrictEqual("hello world");
    });

    it('Should accept Uint8Array', () => {
        let arr = new Uint8Array([1, 2, 3, 4]);
        let bytes = trySerializerForEgress(null, arr);
        expect(bytes instanceof Uint8Array).toStrictEqual(true);
        expect(bytes).toStrictEqual(Buffer.from(arr));
    });

    it('Should serializer an integer as a 32 bit big endian', () => {
        let bytes = trySerializerForEgress(null, 1234567);
        expect(bytes instanceof Uint8Array).toStrictEqual(true);
        expect(bytes.readInt32BE(0)).toStrictEqual(1234567);
    });

    it('Should NOT try to automatically deserialize floats', () => {
        let failed = false;
        try {
            trySerializerForEgress(null, 1.5);
        } catch (e) {
            failed = true;
        }
        expect(failed).toStrictEqual(true);
    });

    it('Should use the custom type if one is provided', () => {
        const UserType = StateFun.jsonType("io.foo.bar/User");
        const aUser = {name: "bob", last: "mop"};

        const bytes = trySerializerForEgress(UserType, aUser);
        const actual = UserType.deserialize(bytes);

        expect(actual).toStrictEqual(aUser);
    });
});
