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

    public IDelete setUpdateColumns(List<ISelectable> columns) {
        throw new NotSupportException();
    }

    public List<ISelectable> getUpdateColumns() {
        return Lists.newArrayList();
    }

    public IDelete setUpdateValues(List<Comparable> values) {
        throw new NotSupportException();
    }

    public List<Comparable> getUpdateValues() {
        return Lists.newArrayList();
    }
}
