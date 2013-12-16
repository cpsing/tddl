package com.taobao.tddl.optimizer.core.plan.bean;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.bean.BindVal;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IParallelizableQueryTree;

public abstract class QueryTree extends DataNodeExecutor<IQueryTree> implements IParallelizableQueryTree<IQueryTree> {

    protected IFilter           valueFilter;
    protected IFilter           havingFilter;
    protected List<IOrderBy>    orderBys             = Collections.emptyList();
    protected List<IOrderBy>    groupBys             = Collections.emptyList();
    protected Comparable        limitFrom;
    protected Comparable        limitTo;
    protected List<ISelectable> columns;
    protected String            alias;
    /**
     * 查询模式，并行？串行？
     */
    protected QUERY_CONCURRENCY queryConcurrency     = QUERY_CONCURRENCY.SEQUENTIAL;

    /**
     * 能否被合并成一条sql，默认可以
     */
    protected Boolean           canMerge             = true;

    /**
     * 是否显式使用临时表，默认不可以
     */
    protected Boolean           useTempTableExplicit = false;
    protected Boolean           isSubQuery           = false;
    protected boolean           isTopQuery           = false;

    public IFilter getValueFilter() {
        return valueFilter;
    }

    public IQueryTree setValueFilter(IFilter valueFilter) {
        this.valueFilter = valueFilter;
        return this;
    }

    public List<ISelectable> getColumns() {
        return columns;
    }

    public IQueryTree setColumns(List<ISelectable> columns) {
        this.columns = columns;
        return this;
    }

    public IQueryTree setColumns(ISelectable... columns) {
        return setColumns(Arrays.asList(columns));
    }

    public List<IOrderBy> getOrderBys() {
        return orderBys;
    }

    public IQueryTree setOrderBys(List<IOrderBy> orderBys) {
        this.orderBys = orderBys;
        return this;
    }

    public Comparable getLimitFrom() {
        return limitFrom;
    }

    public IQueryTree setLimitFrom(Comparable limitFrom) {
        this.limitFrom = limitFrom;
        return this;
    }

    public Comparable getLimitTo() {
        return limitTo;
    }

    public IQueryTree setLimitTo(Comparable limitTo) {
        this.limitTo = limitTo;
        return this;
    }

    public List<IOrderBy> getGroupBys() {
        return groupBys;
    }

    public IQueryTree setGroupBys(List<IOrderBy> groupBys) {
        this.groupBys = groupBys;
        return this;
    }

    public IQueryTree setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public IQueryTree setCanMerge(Boolean canMerge) {
        this.canMerge = canMerge;
        return this;
    }

    public Boolean canMerge() {
        return canMerge;
    }

    public IQueryTree setUseTempTableExplicit(Boolean isUseTempTable) {
        this.useTempTableExplicit = isUseTempTable;
        return this;
    }

    public Boolean isUseTempTableExplicit() {
        return useTempTableExplicit;
    }

    public Boolean isSubQuery() {
        return isSubQuery;
    }

    public IQueryTree setIsSubQuery(Boolean isSubQuery) {
        this.isSubQuery = isSubQuery;
        return this;
    }

    public IFilter getHavingFilter() {
        return havingFilter;
    }

    public IQueryTree having(IFilter having) {
        this.havingFilter = having;
        return this;
    }

    public boolean isTopQuery() {
        return isTopQuery;
    }

    public IQueryTree setTopQuery(boolean topQuery) {
        this.isTopQuery = topQuery;
        return this;
    }

    public IParallelizableQueryTree setQueryConcurrency(QUERY_CONCURRENCY queryConcurrency) {
        this.queryConcurrency = queryConcurrency;
        return this;
    }

    public QUERY_CONCURRENCY getQueryConcurrency() {
        return queryConcurrency;
    }

    public IQueryTree assignment(Map<Integer, ParameterContext> parameterSettings) {
        if (this.getColumns() != null) {
            for (ISelectable c : this.getColumns()) {
                if (c instanceof ISelectable) {
                    ((ISelectable) c).assignment(parameterSettings);
                }
            }
        }

        IFilter rsf = getValueFilter();
        if (rsf != null) {
            rsf.assignment(parameterSettings);
        }

        IFilter havingFilter = this.getHavingFilter();
        if (havingFilter != null) {
            havingFilter.assignment(parameterSettings);
        }

        Comparable limtFrom = getLimitFrom();
        if (limtFrom != null) {
            if (limtFrom instanceof BindVal) {
                this.setLimitFrom(((BindVal) limtFrom).assignment(parameterSettings));
            }
        }

        Comparable limtTo = getLimitTo();
        if (limtTo != null) {
            if (limtTo instanceof BindVal) {
                this.setLimitTo(((BindVal) limtTo).assignment(parameterSettings));
            }
        }
        return this;
    }

    protected void copySelfTo(QueryTree o) {
        o.setRequestID(this.getRequestID());
        o.setSubRequestID(this.getSubRequestID());
        o.setRequestHostName(this.getRequestHostName());
        o.setColumns(this.getColumns());
        o.setConsistent(this.getConsistent());
        o.setGroupBys(this.getGroupBys());
        o.setLimitFrom(this.getLimitFrom());
        o.setLimitTo(this.getLimitTo());
        o.setOrderBys(this.getOrderBys());
        o.setQueryConcurrency(this.getQueryConcurrency());
        o.setValueFilter(this.getValueFilter());
        o.setAlias(this.getAlias());
        o.setCanMerge(this.canMerge());
        o.setUseTempTableExplicit(this.isUseTempTableExplicit());
        o.setThread(getThread());
        o.having(this.getHavingFilter());
        o.setStreaming(streaming);
        o.setSql(sql);
        o.setIsSubQuery(this.isSubQuery);
        o.executeOn(this.getDataNode());
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

}
