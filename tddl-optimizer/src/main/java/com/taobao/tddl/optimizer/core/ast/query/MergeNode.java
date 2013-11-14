package com.taobao.tddl.optimizer.core.ast.query;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.build.QueryTreeNodeBuilder;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public class MergeNode extends QueryTreeNode {

    private String tableName;

    public void build() {

    }

    public IDataNodeExecutor toDataNodeExecutor() {
        return null;
    }

    public void assignment(Map parameterSettings) {

    }

    public boolean isNeedBuild() {
        return false;
    }

    public String toString(int inden) {
        return null;
    }

    public ASTNode deepCopy() {
        return null;
    }

    public ASTNode copy() {
        return null;
    }

    public List getImplicitOrderBys() {
        return null;
    }

    public QueryTreeNodeBuilder getBuilder() {
        return null;
    }

    public String getName() {
        return null;
    }

}
