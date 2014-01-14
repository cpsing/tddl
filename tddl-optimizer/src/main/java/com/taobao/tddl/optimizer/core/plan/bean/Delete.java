package com.taobao.tddl.optimizer.core.plan.bean;

import java.util.List;

import com.google.common.collect.Lists;
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
        return Lists.newArrayList();
    }

    @Override
    public IDelete setUpdateValues(List<Object> values) {
        throw new NotSupportException();
    }

    @Override
    public List<Object> getUpdateValues() {
        return Lists.newArrayList();
    }
}
