package com.taobao.tddl.repo.mysql.sqlconvertor;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.jdbc.ParameterContext;

public class MyParamSqlContext extends SqlAndParam implements Cloneable {

    String        beforeTableStr;
    String        afterTableStr;
    String        whereStr;
    String        tableName;
    AtomicInteger bindValSequence;

    public String getSql() {
        StringBuilder sb = new StringBuilder(beforeTableStr);
        sb.append(tableName).append(afterTableStr);
        if (!StringUtils.isBlank(whereStr)) sb.append(" where ").append(whereStr);
        return this.sql = sb.toString();
    }

    @Override
    public MyParamSqlContext clone() {
        MyParamSqlContext clone;
        try {
            clone = (MyParamSqlContext) super.clone();
        } catch (CloneNotSupportedException e) {
            clone = new MyParamSqlContext();
        }
        clone.param = new HashMap<Integer, ParameterContext>();
        clone.param.putAll(this.param);
        clone.setBindValSequence(new AtomicInteger(this.getBindValSequence().get()));
        return clone;
    }

    // ================================getter setter=====================
    public String getBeforeTableStr() {
        return beforeTableStr;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setBeforeTableStr(String beforeTableStr) {
        this.beforeTableStr = beforeTableStr;
    }

    public String getAfterTableStr() {
        return afterTableStr;
    }

    public void setAfterTableStr(String afterTableStr) {
        this.afterTableStr = afterTableStr;
    }

    public String getWhereStr() {
        return whereStr;
    }

    public void setWhereStr(String whereStr) {
        this.whereStr = whereStr;
    }

    public AtomicInteger getBindValSequence() {
        return bindValSequence;
    }

    public void setBindValSequence(AtomicInteger bindValSequence) {
        this.bindValSequence = bindValSequence;
    }

    @Override
    public String toString() {
        return "MyParamSqlContext [sql=" + this.getSql() + ", param=" + param + "]";
    }

}
