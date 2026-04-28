// SPDX-License-Identifier: Apache-2.0

import {describe, expect} from '@jest/globals'
import {StateFun} from '../src/statefun';

function assertThrows(ex: any, fn: () => any) {
    let failed = false;
    try {
        fn();
    } catch (e) {
        failed = true;
    }
    ex(failed).toStrictEqual(true);
}


describe('StateFun', () => {
    it('Should demonstrate a usage of binding a function', () => {
        let sf = new StateFun();

        sf.bind({
            typename: "com.foo.fns/greeter",
            specs: [
                {
                    name: "seen",
                    type: StateFun.intType()
                }
            ],

            fn(context, message) {
            }
        });
    });

    it('Should demonstrate a usage of binding a function with no states', () => {
        let sf = new StateFun();

        sf.bind({
            typename: "com.foo.fns/greeter",
            fn(context, message) {
            }
        });
    });


    it('Should fail with a bad typename', () => {
        assertThrows(expect, () => {

            let sf = new StateFun();
            sf.bind({
                typename: "/greeter",
                fn(context, message) {
                }
            });


        });
    });

    it('Should fail with a bad spec name', () => {
        assertThrows(expect, () => {

            let sf = new StateFun();
            sf.bind({
                typename: "foo/greeter",
                specs: [{name: "a b", type: StateFun.intType()}],
                fn(context, message) {
                }
            });

        });
    });

    it('Should fail with duplicate spec names', () => {
        assertThrows(expect, () => {

            let sf = new StateFun();
            sf.bind({
                typename: "foo/greeter",
                specs: [
                    {name: "a", type: StateFun.intType()},
                    {name: "a", type: StateFun.intType()}
                ],
                fn(context, message) {
                }
            });

        });
    });

});