// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation

import {ValueSpec} from "../src/core";
import {StateFun} from "../src/statefun";
import {describe, expect} from '@jest/globals'

describe('ValueSpec', () => {
    it('Should be constructed correctly from a js object', () => {

        const spec = ValueSpec.fromOpts({name: "hello", type: StateFun.intType(), expireAfterCall: 123});

        expect(spec.name).toStrictEqual("hello");
        expect(spec.type).toStrictEqual(StateFun.intType());
        expect(spec.expireAfterCall).toStrictEqual(123);
        expect(spec.expireAfterWrite).toStrictEqual(-1);
    });

    it('Should be constructed correctly from a js object with expireAfterWrite', () => {
        const spec = ValueSpec.fromOpts({name: "hello", type: StateFun.intType(), expireAfterWrite: 123});

        expect(spec.name).toStrictEqual("hello");
        expect(spec.type).toStrictEqual(StateFun.intType());
        expect(spec.expireAfterCall).toStrictEqual(-1);
        expect(spec.expireAfterWrite).toStrictEqual(123);
    });

});
