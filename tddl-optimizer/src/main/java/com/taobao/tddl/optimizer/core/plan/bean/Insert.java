package com.taobao.tddl.optimizer.core.plan.bean;

import com.taobao.tddl.optimizer.core.plan.dml.IInsert;

public class Insert extends Put<IInsert> implements IInsert {

    public Insert(){
        putType = PUT_TYPE.INSERT;
    }
}
