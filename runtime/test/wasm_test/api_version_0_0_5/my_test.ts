export * from './common/global'
import { Value, ValueKind, TypedMapEntry, TypedMap, Entity, JSONValueKind, JSONValue } from './common/types'


export class UnitTestType{
    str_pref: string;
    under_test: boolean;
    str_suff: string;

    constructor(str_pref: string, under_test: boolean, str_suff:string) {
        this.str_pref = str_pref;
        this.under_test = under_test;
        this.str_suff = str_suff;
    }
}


//export function my_test(parm: UnitTestType): void {
export function my_test(p: Value): void {
    let parm = changetype<UnitTestType>(p as u32);

    assert(parm.str_pref == "pref`", "parm.str_pref: Assertion failed!")
    assert(parm.under_test == true, "parm.under_test: Assertion failed!")
    assert(parm.str_suff == "suff`", "parm.str_suff: Assertion failed!")
}
