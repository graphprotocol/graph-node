export * from './common/global'


export class UnitTestType{
    str_pref: string;
    under_test: boolean;
    str_suff: string;
}



export function my_test(parm: UnitTestType): void {
    assert(parm.str_pref == "pref`", "parm.str_pref: Assertion failed!")
    assert(parm.under_test == true, "parm.under_test: Assertion failed!")
    assert(parm.str_suff == "suff`", "parm.str_suff: Assertion failed!")
}
