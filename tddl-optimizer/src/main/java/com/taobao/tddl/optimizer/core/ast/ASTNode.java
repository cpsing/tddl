package com.taobao.tddl.optimizer.core.ast;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * 可优化的语法树
 * 
 * @author jianghang 2013-11-8 下午2:30:25
 * @since 5.1.0
 */
public abstract class ASTNode<RT extends ASTNode> {

    protected String dataNode = null;
    protected Object extra;
    // TODO 该属性待定
    protected String sql;

    public abstract void build();

    /**
     * 构造执行计划
     */
    public abstract IDataNodeExecutor toDataNodeExecutor() throws QueryException;

    public abstract void assignment(Map<Integer, ParameterContext> parameterSettings);

    public abstract boolean isNeedBuild();

    public String getDataNode() {
        return dataNode;
    }

    public RT executeOn(String dataNode) {
        this.dataNode = dataNode;
        return (RT) this;
    }

    public String getSql() {
        return this.sql;
    }

    public RT setSql(String sql) {
        this.sql = sql;
        return (RT) this;
    }

    public abstract String toString(int inden);

    public Object getExtra() {
        return this.extra;
    }

    public void setExtra(Object obj) {
        this.extra = obj;
    }

    // ----------------- 复制 ----------------

    public abstract RT deepCopy();

    public abstract RT copy();

}
