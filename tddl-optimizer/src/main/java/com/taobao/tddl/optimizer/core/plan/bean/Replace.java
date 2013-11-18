package com.taobao.tddl.optimizer.core.plan.bean;

import com.taobao.tddl.optimizer.core.plan.dml.IReplace;

public class Replace extends Put<IReplace> implements IReplace {

    public Replace(){
        putType = PUT_TYPE.REPLACE;
    }
}
