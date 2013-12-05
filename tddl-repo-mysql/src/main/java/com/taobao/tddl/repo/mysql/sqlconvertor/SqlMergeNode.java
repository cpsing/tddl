package com.taobao.tddl.repo.mysql.sqlconvertor;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

public class SqlMergeNode {

    /**
     * order by 条件
     */
    protected List<IOrderBy>              orderBy  = Collections.emptyList();

    /**
     * 进行合并后，是否要丢弃某些数据
     */
    protected Long                        limitFrom;

    /**
     * 进行合并后，取多少条记录
     */
    protected Long                        limitTo;

    /**
     * group by 什么条件
     */
    protected List<IOrderBy>              groupBys = Collections.emptyList();

    /**
     * 返回列的情况。可能是函数，可能是其他操作，也可能是嵌套的函数 请参见
     * com.taobao.ustore.common.inner.bean.IFunction
     * com.taobao.ustore.common.inner.bean.IColumn
     */
    protected List<ISelectable>           columns;

    protected Map<String/* 执行节点 */, Sqls> subQuerys;

    public List<IOrderBy> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(List<IOrderBy> orderBy) {
        this.orderBy = orderBy;
    }

    public Long getLimitFrom() {
        return limitFrom;
    }

    public void setLimitFrom(Comparable limitFrom) {
        this.limitFrom = (Long) limitFrom;
    }

    public Long getLimitTo() {
        return limitTo;
    }

    public void setLimitTo(Comparable limitTo) {
        this.limitTo = (Long) limitTo;
    }

    public List<IOrderBy> getGroupBys() {
        return groupBys;
    }

    public void setGroupBys(List<IOrderBy> groupBys) {
        this.groupBys = groupBys;
    }

    public List<ISelectable> getColumns() {
        return columns;
    }

    public void setColumns(List<ISelectable> columns) {
        this.columns = columns;
    }

    public Map<String, Sqls> getSubQuerys() {
        return subQuerys;
    }

    public void setSubQuerys(Map<String, Sqls> subQuerys) {
        this.subQuerys = subQuerys;
    }

    @Override
    public String toString() {
        final int maxLen = 10;
        StringBuilder builder = new StringBuilder();
        builder.append("SqlMergeNode [\n\t");
        if (orderBy != null) {
            builder.append("orderBy=");
            builder.append(toString(orderBy, maxLen));
            builder.append(", \n\t");
        }
        if (limitFrom != null) {
            builder.append("limitFrom=");
            builder.append(limitFrom);
            builder.append(", \n\t");
        }
        if (limitTo != null) {
            builder.append("limitTo=");
            builder.append(limitTo);
            builder.append(", \n\t");
        }
        if (groupBys != null) {
            builder.append("groupBys=");
            builder.append(toString(groupBys, maxLen));
            builder.append(", \n\t");
        }
        if (columns != null) {
            builder.append("columns=");
            builder.append(toString(columns, maxLen));
            builder.append(", \n\t");
        }
        if (subQuerys != null) {
            builder.append(toString(subQuerys.entrySet(), maxLen));
        }
        builder.append("\n]");
        return builder.toString();
    }

    private String toString(Collection<?> collection, int maxLen) {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        int i = 0;
        for (Iterator<?> iterator = collection.iterator(); iterator.hasNext() && i < maxLen; i++) {
            if (i > 0) builder.append(", \n");
            builder.append(iterator.next());
        }
        builder.append("]");
        return builder.toString();
    }

}
