package com.taobao.tddl.optimizer.core.ast.dml;

import java.util.Map;

import com.taobao.tddl.common.utils.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public class UpdateNode extends DMLNode<UpdateNode> {

    public void build() {

    }

    public IDataNodeExecutor toDataNodeExecutor() {
        return null;
    }

    public void assignment(Map<Integer, ParameterContext> parameterSettings) {

    }

    public boolean isNeedBuild() {
        return false;
    }

    public String toString(int inden) {
        return null;
    }

    public UpdateNode deepCopy() {
        return null;
    }

    public UpdateNode copy() {
        return null;
    }

}
