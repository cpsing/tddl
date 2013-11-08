package com.taobao.tddl.optimizer.core.plan;

public interface IPut<RT extends IPut> extends IDataNodeExecutor<RT> {

    public enum PUT_TYPE {
        REPLACE, INSERT, DELETE, UPDATE;
    }
}
