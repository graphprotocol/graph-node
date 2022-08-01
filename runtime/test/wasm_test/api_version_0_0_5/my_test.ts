export * from './common/global'

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

export function my_test(p: UnitTestType): void {
     assert(p.str_pref == "pref", "parm.str_pref: Assertion failed!");
     assert(p.under_test == true, "parm.under_test: Assertion failed!");
     assert(p.str_suff == "suff", "parm.str_suff: Assertion failed!");
}
