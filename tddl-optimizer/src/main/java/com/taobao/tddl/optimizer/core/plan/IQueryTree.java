package com.taobao.tddl.optimizer.core.plan;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * @since 5.0.0
 */
public interface IQueryTree<RT extends IQueryTree> extends IDataNodeExecutor<RT> {

    public enum LOCK_MODEL {
        SHARED_LOCK, EXCLUSIVE_LOCK;
    }

    /**
     * valueFilter 简单来说就是对所有数据，一行一行进行检索的filter.
     * 可以保证所有bool条件都能够查询，但性能较慢，一般来说尽可能先使用key filter之后再使用value filter
     * 
     * @return
     */
    public IFilter getValueFilter();

    /**
     * valueFilter 简单来说就是对所有数据，一行一行进行检索的filter.
     * 可以保证所有bool条件都能够查询，但性能较慢，一般来说尽可能先使用key filter之后再使用value filter
     * 
     * @param valueFilter
     * @return
     */
    public RT setValueFilter(IFilter valueFilter);

    /**
     * 当前查询中应该使用的查询列。只有在这个出现的列，才会被允许展现
     * 
     * @return
     */
    public List<ISelectable> getColumns();

    /**
     * 设置当前查询节点的column filters ,只有出现在columnFilters里面的列，才可以出现在这个结果集的展现列中。
     * 
     * @param columns
     * @return
     */
    public RT setColumns(List<ISelectable> columns);

    /**
     * 设置当前查询节点的column filters ,只有出现在columnFilters里面的列，才可以出现在这个结果集的展现列中。
     * 
     * @param column_filter
     * @return
     */
    public RT setColumns(ISelectable... column_filter);

    public List<IOrderBy> getOrderBys();

    /**
     * 设置order by条件 如果有order by ，那么数据必须按照order by
     * 条件进行数据重排，除非数据本身key就是按照该数据结构进行排列的。
     * 
     * @param orderBy
     * @return
     */
    public RT setOrderBys(List<IOrderBy> orderBys);

    /**
     * 从哪里开始
     * 
     * @return
     */
    public Comparable getLimitFrom();

    /**
     * 设置这次数据从第几个开始取
     * 
     * @param limitFrom
     * @return
     */
    public RT setLimitFrom(Comparable limitFrom);

    /**
     * 到哪里结束
     * 
     * @return
     */
    public Comparable getLimitTo();

    /**
     * 设置这次数据取到第几个。
     * 
     * @param limitTo
     * @return
     */
    public RT setLimitTo(Comparable limitTo);

    /**
     * 结果集按照什么进行排序。 如果无序，那么order by里面的asc或desc是空
     * 
     * @return
     */
    public List<IOrderBy> getGroupBys();

    /**
     * 设置按照什么函数进行聚类
     * 
     * @param groupBy
     * @return
     */
    public RT setGroupBys(List<IOrderBy> groupBys);

    /**
     * 设置当前query的别名
     * 
     * @param alias
     * @return
     */
    public RT setAlias(String alias);

    /**
     * 获取别名
     * 
     * @return
     */
    public String getAlias();

    public RT assignment(Map<Integer, ParameterContext> parameterSettings);

    /**
     * <pre>
     * 在处理join的时候，会出现未决节点 比如 数据IDX_PRI: pk - > col1,col2,col3. 
     * 数据按照pk进行切分索引IDX_COL1: col1->pk 数据按照col1进行切分 
     * 1. 那么索引查找的时候，会生成一个join 先查询IDX_COL1.
     * 2. 然后根据IDX_COL1的"结果"，来决定应该去哪些pk索引的数据节点上进行查询（因为IDX_PRI也是分了多个机器的）。
     * 所以这时候IDX_PRI是不能预先知道自己在这次查询中应该去查哪些节点的。 这时候。 针对IDX_PRI的查询节点，就是未决节点,这时候canMerge为true.
     * </pre>
     * 
     * @param canMerge
     */
    public RT setCanMerge(Boolean canMerge);

    /**
     * <pre>
     * 在处理join的时候，会出现未决节点 比如 数据IDX_PRI: pk - > col1,col2,col3. 
     * 数据按照pk进行切分索引IDX_COL1: col1->pk 数据按照col1进行切分 
     * 1. 那么索引查找的时候，会生成一个join 先查询IDX_COL1.
     * 2. 然后根据IDX_COL1的"结果"，来决定应该去哪些pk索引的数据节点上进行查询（因为IDX_PRI也是分了多个机器的）。
     * 所以这时候IDX_PRI是不能预先知道自己在这次查询中应该去查哪些节点的。 这时候。 针对IDX_PRI的查询节点，就是未决节点,这时候canMerge为true.
     * </pre>
     * 
     * @return
     */
    public Boolean canMerge();

    /**
     * 是否显式指定使用临时表
     * 
     * @param isUseTempTable
     */
    public RT setUseTempTableExplicit(Boolean isUseTempTable);

    /**
     * 是否显式指定使用临时表
     * 
     * @return
     */
    public Boolean isUseTempTableExplicit();

    /**
     * 是否是个子查询
     * 
     * @return
     */
    public Boolean isSubQuery();

    public RT setIsSubQuery(Boolean isSubQuery);

    /**
     * having 子句支持
     * 
     * @return
     */
    public IFilter getHavingFilter();

    /**
     * having 子句支持。用在groupby 后面
     * 
     * @param having
     * @return
     */
    public RT having(IFilter having);

    /**
     * 是否是最外层的查询
     * 
     * @return
     */
    public boolean isTopQuery();

    public RT setTopQuery(boolean topQuery);

}
