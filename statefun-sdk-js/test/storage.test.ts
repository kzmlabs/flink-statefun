// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

import {describe, expect} from '@jest/globals'
import {StateFun} from '../src/statefun';
import {Value, AddressScopedStorageFactory} from '../src/storage';
import {TypedValueSupport} from "../src/types";
import {Type} from "../src/core";
import "../src/generated/request-reply_pb";

function stateFrom<T>(name: string, tpe: Type<T>, obj: T): any {
    // noinspection JSUnresolvedVariable
    let pv = new global.proto.io.statefun.sdk.reqreply.ToFunction.PersistedValue();
    pv.setStateName(name);
    pv.setStateValue(TypedValueSupport.toTypedValue(obj, tpe))
    return pv;
}

describe('Value Test', () => {
    it('should demonstrate a simple usage', () => {
        const incomingType = StateFun.intType();
        let incomingState = stateFrom("seen", incomingType, 123);

        let v = Value.fromState(incomingState, incomingType);

        expect(v.getValue()).toStrictEqual(123);

        v.setValue(v.getValue()! + 1);
        expect(v.getValue()).toStrictEqual(124);

        v.setValue(null);
        expect(v.getValue()).toStrictEqual(null);

        v.setValue(5);
        expect(v.getValue()).toStrictEqual(5);
    });

    it('should round trip successfully', () => {
        const incomingType = StateFun.intType();
        let incomingState = stateFrom("seen", incomingType, 123);

        let v = Value.fromState(incomingState, incomingType);
        v.setValue(v.getValue()! + 1) // value should be 124
        const mutation = v.asMutation();

        expect(mutation).not.toBeNull();
        expect(mutation!.getMutationType()).toStrictEqual(1);

        const actual = TypedValueSupport.parseTypedValue(mutation!.getStateValue(), incomingType);

        expect(actual).toStrictEqual(124);
    });

    it('should not produce a mutation if nothing has changed.', () => {
        const incomingType = StateFun.intType();
        let incomingState = stateFrom("seen", incomingType, 123);

        let v = Value.fromState(incomingState, incomingType);
        // do nothing
        const mutation = v.asMutation();

        expect(mutation).toStrictEqual(null);
    });

    it('should produce mutation of type DELETE', () => {
        const incomingType = StateFun.intType();
        let incomingState = stateFrom("seen", incomingType, 123);


        let v = Value.fromState(incomingState, incomingType);
        v.setValue(null); // acts as delete.
        const mutation = v.asMutation();

        expect(mutation).not.toBeNull();
        expect(mutation!.getMutationType()).toStrictEqual(0);
    });


    it('AddressScopedStorageFactory should produce an object with registered states as attributes', () => {
        const incomingType = StateFun.intType();
        let incomingState1 = stateFrom("seen", incomingType, 123);
        let incomingState2 = stateFrom("idle", incomingType, 456);

        let v1 = Value.fromState(incomingState1, incomingType);
        let v2 = Value.fromState(incomingState2, incomingType);

        let storage = AddressScopedStorageFactory.create([v1, v2]);

        storage.seen += 1;
        storage.idle += 1;

        const mutations = AddressScopedStorageFactory.collectMutations([v1, v2]);
        expect(mutations.length).toStrictEqual(2);
    });
});
