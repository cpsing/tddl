package com.taobao.tddl.optimizer.core.ast;

import java.util.Map;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * 可优化的语法树
 * 
 * @since 5.0.0
 */
public abstract class ASTNode<RT extends ASTNode> implements Comparable {

    protected String  dataNode  = null; // 数据处理节点,比如group name
    protected Object  extra;            // 比如唯一标识，join merge join中使用
    protected boolean broadcast = false; // 是否为广播表
    protected String  sql;

    /**
     * <pre>
     * 1. 结合table meta信息构建结构树中完整的column字段
     * 2. 处理join/merge的下推处理
     * </pre>
     */
    public abstract void build();

    /**
     * 需要预先执行build.构造执行计划
     */
    public abstract IDataNodeExecutor toDataNodeExecutor() throws QueryException;

    /**
     * 处理bind val
     */
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

    public Object getExtra() {
        return this.extra;
    }

    public void setExtra(Object obj) {
        this.extra = obj;
    }

    public boolean isBroadcast() {
        return broadcast;
    }

    public void setBroadcast(boolean broadcast) {
        this.broadcast = broadcast;
    }

    public int compareTo(Object arg) {
        // 主要是将自己包装为Comparable对象，可以和Number/string类型具有相同的父类，构建嵌套的查询树
        throw new NotSupportException();
    }

    public abstract String toString(int inden);

    // ----------------- 复制 ----------------

    public abstract RT deepCopy();

    public abstract RT copy();

}
