package com.taobao.tddl.optimizer.core.plan.bean;

import com.taobao.tddl.optimizer.core.plan.dml.IUpdate;

public class Update extends Put<IUpdate> implements IUpdate {

    public Update(){
        putType = PUT_TYPE.UPDATE;
    }
}
