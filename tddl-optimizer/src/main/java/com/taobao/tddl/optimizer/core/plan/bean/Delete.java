package com.taobao.tddl.optimizer.core.plan.bean;

import java.util.List;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.dml.IDelete;

public class Delete extends Put<IDelete> implements IDelete {

    public Delete(){
        putType = PUT_TYPE.DELETE;
    }

    @Override
    public IDelete setUpdateColumns(List<ISelectable> columns) {
        throw new NotSupportException();
    }

    @Override
    public List<ISelectable> getUpdateColumns() {
        throw new NotSupportException();
    }

    @Override
    public IDelete setUpdateValues(List<Comparable> values) {
        throw new NotSupportException();
    }

    @Override
    public List<Comparable> getUpdateValues() {
        throw new NotSupportException();
    }
}
