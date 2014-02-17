package com.taobao.tddl.executor.handler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.DistinctCursor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.monitor.Monitor;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author mengshi.sunmengshi 2013-12-5 上午11:06:01
 * @since 5.0.0
 */
public abstract class QueryHandlerCommon extends HandlerCommon {

    public QueryHandlerCommon(){
        super();
    }

    /**
     * 完全匹配
     */
    public static int    MATCH        = 0;

    /**
     * 前缀匹配
     */
    public static int    PREFIX_MATCH = 1;

    /**
     * 不匹配
     */
    public static int    NOT_MATCH    = -1;
    public static Logger logger       = LoggerFactory.getLogger(QueryHandlerCommon.class);

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        long time = System.currentTimeMillis();
        // 先做查询

        ISchematicCursor cursor = doQuery(null, executor, executionContext);

        if (executor.getSql() == null) {
            IQueryTree IQueryTree = (IQueryTree) executor;

            cursor = processValueFilter(cursor, executionContext, IQueryTree);

            cursor = processGroupByAndOrderBy(cursor, executionContext, IQueryTree);

            cursor = processLimitFromTo(cursor, executionContext, IQueryTree);

            cursor = processColumnAndAlias(cursor, executionContext, IQueryTree);
        }
        time = Monitor.monitorAndRenewTime(Monitor.KEY1, Monitor.ServerQuery, Monitor.Key3Success, time);
        return cursor;
    }

    /**
     * <pre>
     * 首先获取四个关键的属性 
     * 1. groupby 
     * 2. agg columns 算法 
     * 3. distinct? 
     * 4. order by 
     * 5. merge
     * </pre>
     */
    protected ISchematicCursor processGroupByAndOrderBy(ISchematicCursor cursor, ExecutionContext executionContext,
                                                        IQueryTree IQueryTree) throws TddlException {
        // 处理 group by 和aggregate function. cursor =
        cursor = processGroupByAndAggregateFunction(cursor, IQueryTree, executionContext);

        cursor = processDistinct(cursor, IQueryTree, executionContext);

        cursor = processHavingFilter(cursor, executionContext, IQueryTree);
        // 接着处理排序
        cursor = processOrderBy(cursor, IQueryTree.getOrderBys(), executionContext, IQueryTree, true);
        return cursor;
    }

    private ISchematicCursor processDistinct(ISchematicCursor cursor, IQueryTree IQueryTree,
                                             ExecutionContext executionContext) throws TddlException {

        if (isDistinct(IQueryTree)) {
            cursor = processOrderBy(cursor, getOrderBy(IQueryTree.getColumns()), executionContext, IQueryTree, false);
            cursor = new DistinctCursor(cursor, getOrderBy(IQueryTree.getColumns()));
        }

        return cursor;
    }

    boolean isDistinct(IQueryTree qc) {
        for (Object c : qc.getColumns()) {
            if (c instanceof ISelectable && ((ISelectable<IColumn>) c).isDistinct()) {
                return true;
            }
        }

        return false;
    }

    /**
     * 处理 group by 和aggregate function.
     * 
     * <pre>
     * cursor = processGroupByAndAggregateFunction(context, cursor, IQueryTree, executionContext);
     * // 接着处理排序
     * cursor = processOrderBy(cursor, IQueryTree.getOrderBy(), executionContext, IQueryTree);
     * return cursor;
     * </pre>
     */
    protected abstract ISchematicCursor doQuery(ISchematicCursor cursor, IDataNodeExecutor executor,
                                                ExecutionContext executionContext) throws TddlException;

    /**
     * 左数据集和右数据集，排序是否相同。
     * 
     * @param o1
     * @param o2
     * @return
     */
    protected boolean equalsIOrderBy(IOrderBy o1, IOrderBy o2) {
        IColumn c1 = ExecUtils.getColumn(o1.getColumn());
        IColumn c2 = ExecUtils.getColumn(o2.getColumn());
        return StringUtils.equalsIgnoreCase(c1.getTableName(), c2.getTableName())
               && c1.getColumnName().equals(c2.getColumnName()) && o1.getDirection() == o2.getDirection();
    }

    protected List<IFunction> getMergeAggregates(List retColumns) {
        return getAggregatesCommon(retColumns, true);
    }

    /**
     * 从select [columns] 里面获取aggregate functions
     * 
     * @param retColumns
     * @return
     */
    protected List<IFunction> getAggregatesCommon(List retColumns, boolean isMergeAggregates) {
        List<IFunction> aggregates = new ArrayList<IFunction>();
        for (int i = 0; i < retColumns.size(); i++) {
            Object o = retColumns.get(i);
            if (o instanceof IFunction) {
                // 如果retColumn中出现了函数名字，那么进入这个逻辑
                IFunction f = (IFunction) o;
                if (FunctionType.Aggregate.equals(f.getFunctionType())) {
                    aggregates.add(f);
                } else {
                    List<IFunction> aggregateInThisScalar = new ArrayList();
                    findAggregateFunctionsInScalar(f, aggregateInThisScalar);
                    if (!aggregateInThisScalar.isEmpty()) {
                        aggregates.add(f);
                    }
                }
            }
        }
        return aggregates;
    }

    private void findAggregateFunctionsInScalar(IFunction s, List<IFunction> res) {
        if (IFunction.FunctionType.Aggregate.equals(s.getFunctionType())) {
            res.add(s);
        }

        for (Object arg : s.getArgs()) {
            if (arg instanceof IFunction) {
                this.findAggregateFunctionsInScalar((IFunction) arg, res);
            }
        }

    }

    /**
     * <pre>
     * group by和aggregate Function。 
     * 对单机来说，原则就是尽可能的使用索引完成count max min的功能。
     * 参考的关键条件有：
     * 1. 是否需要group by 
     * 2. 是什么aggregate. 
     * 3. 是否需要distinct 
     * 4. 是否是merge节点
     * </pre>
     */
    protected ISchematicCursor processGroupByAndAggregateFunction(ISchematicCursor cursor, IQueryTree IQueryTree,
                                                                  ExecutionContext executionContext)
                                                                                                    throws TddlException {
        // 是否带有group by 列。。
        List<IOrderBy> groupBycols = IQueryTree.getGroupBys();
        boolean closeResultCursor = executionContext.isCloseResultSet();
        final IRepository repo = executionContext.getCurrentRepository();

        List retColumns = getEmptyListIfRetColumnIsNull(IQueryTree);
        List<IFunction> _agg = getAggregates(retColumns);
        // 接着处理group by
        if (groupBycols != null && !groupBycols.isEmpty()) {
            // group by之前需要进行排序，按照group by列排序
            cursor = processOrderBy(cursor, (groupBycols), executionContext, IQueryTree, false);
        }

        cursor = executeAgg(cursor, IQueryTree, closeResultCursor, repo, _agg, groupBycols, executionContext);
        return cursor;
    }

    protected ISchematicCursor executeAgg(ISchematicCursor cursor, IDataNodeExecutor executor,
                                          boolean closeResultCursor, IRepository repo, List<IFunction> aggregates,
                                          List<IOrderBy> groupBycols, ExecutionContext executionContext)
                                                                                                        throws TddlException {
        List<ISelectable> _retColumns = null;
        if (executor instanceof IQuery) {
            _retColumns = ((IQuery) executor).getColumns();
        } else if (executor instanceof IJoin) {
            _retColumns = ((IJoin) executor).getColumns();
        } else if (executor instanceof IMerge) {
            _retColumns = ((IMerge) executor).getColumns();
        }

        if (_retColumns != null) {
            if ((aggregates != null && !aggregates.isEmpty()) || (groupBycols != null && !groupBycols.isEmpty())) {
                cursor = repo.getCursorFactory().aggregateCursor(executionContext,
                    cursor,
                    aggregates,
                    groupBycols,
                    _retColumns,
                    false);
            }
        }
        return cursor;
    }

    protected List getEmptyListIfRetColumnIsNull(IQueryTree IQueryTree) {
        return IQueryTree.getColumns() == null ? Collections.EMPTY_LIST : IQueryTree.getColumns();
    }

    protected List<IFunction> getAggregates(List retColumns) {
        return getAggregatesCommon(retColumns, false);
    }

    /**
     * 根据列名，生成order by 条件.永远是正向。
     * 
     * @param columns
     * @return
     */
    protected static final List<IOrderBy> getOrderBy(List<ISelectable> columns) {
        if (columns == null) {
            columns = Collections.EMPTY_LIST;
        }
        List<IOrderBy> orderBys = new ArrayList<IOrderBy>(columns.size());
        for (Object cobj : columns) {
            IColumn c = ExecUtils.getColumn(cobj);
            orderBys.add(ASTNodeFactory.getInstance().createOrderBy().setColumn(c).setDirection(true));
        }
        return orderBys;
    }

    protected ISchematicCursor processOrderBy(ISchematicCursor cursor, List<IOrderBy> ordersInRequest,
                                              ExecutionContext executionContext, IQueryTree IQueryTree,
                                              boolean needOrderMatch) throws TddlException {
        IRepository repo = executionContext.getCurrentRepository();
        boolean hasOrderBy = ordersInRequest != null && !ordersInRequest.isEmpty();
        if (!hasOrderBy) {
            return cursor;
        }
        OrderByResult orderByResult = chooseOrderByMethod(cursor, ordersInRequest, executionContext, needOrderMatch);
        switch (orderByResult) {
            case temporaryTable: {
                return repo.getCursorFactory().tempTableSortCursor(executionContext,
                    cursor,
                    ordersInRequest,
                    true,
                    IQueryTree.getRequestID());
            }
            case reverseCursor:
                return repo.getCursorFactory().reverseOrderCursor(executionContext, cursor);
            case normal:
                if (requestCMTabisCursorCMTab(ordersInRequest, cursor.getOrderBy())) {
                    return cursor;
                } else {
                    return repo.getCursorFactory().setOrderCursor(executionContext, cursor, ordersInRequest);
                }
            default:
                throw new IllegalArgumentException("should not be here");
        }
    }

    private boolean requestCMTabisCursorCMTab(List<IOrderBy> ordersInRequest, List<IOrderBy> ordersInCursor) {
        for (int i = 0; i < ordersInRequest.size(); i++) {
            IOrderBy orderInCursor = ordersInCursor.get(i);
            IOrderBy orderInRequest = ordersInRequest.get(i);
            if (!TStringUtil.equals(orderInCursor.getColumn().getTableName(), orderInRequest.getColumn().getTableName())) {
                return false;
            }
        }
        return true;
    }

    /**
     * 比较两个排序，排序可不相同
     * 
     * @param o1
     * @param o2
     * @return
     */
    protected static boolean isTwoOrderByMatched(IOrderBy o1, IOrderBy o2) {
        IColumn c1 = ExecUtils.getIColumn(o1.getColumn());
        IColumn c2 = ExecUtils.getIColumn(o2.getColumn());
        boolean columnMatch = c1 != null
                              && c2 != null
                              && (StringUtils.equals(c1.getTableName(), c2.getTableName()))
                              && (c1.getColumnName().equals(c2.getColumnName()) || (c1.getAlias() != null && c1.getAlias()
                                  .equals(c2.getColumnName())));

        return columnMatch;
    }

    /**
     * @param ordersInCursor
     * @param ordersInRequest
     * @param executionContext
     * @param needOrderMatch
     * 为true时，ordersInRequest和ordersInCursor必须顺序一致才会认定为不需要排序
     * 为false时，只要ordersInCursor包含ordersInRequest中的列即可
     * ，不必要顺序一致，亦不考虑order的direction
     * @return
     */
    protected static OrderByResult chooseOrderByMethod(List<IOrderBy> ordersInCursor, List<IOrderBy> ordersInRequest,
                                                       ExecutionContext executionContext, boolean needOrderMatch) {

        if (!needOrderMatch) {
            return chooseOrderByMethodNotNeedOrderMatch(ordersInCursor, ordersInRequest, executionContext);
        }

        if (ordersInRequest != null && ordersInRequest.size() <= ordersInCursor.size()) {
            // 察看order顺序
            int requestOrderSize = ordersInRequest.size();
            boolean first = true;
            boolean firstOrderInCursor = true;
            OrderByResult ret = null;
            for (int i = 0; i < requestOrderSize; i++) {

                // 在当前cursor(也就是原本数据中的order by
                IOrderBy orderInCursor = ordersInCursor.get(i);
                IOrderBy orderInRequest = ordersInRequest.get(i);
                Boolean bool = orderInCursor.getDirection();
                if (bool == null) {
                    return OrderByResult.temporaryTable;
                }
                if (first) {
                    first = false;
                    firstOrderInCursor = orderInCursor.getDirection();
                }

                boolean columnNotMatch = !isTwoOrderByMatched(orderInCursor, orderInRequest);

                if (columnNotMatch) {
                    return OrderByResult.temporaryTable;
                }

                /**
                 * <pre>
                 * 1.cursor中的顺序全部相同，并且request中的顺序也全部相同，才可能使用reverse
                 * 2.cursor中的顺序不同，但与request中的顺序相同，可以使用normal 
                 * 3. 其他都是临时表
                 * </pre>
                 */
                if (firstOrderInCursor == orderInCursor.getDirection()) {
                    if (orderInCursor.getDirection() != orderInRequest.getDirection()) {
                        if (ret == null) ret = OrderByResult.reverseCursor;
                        else if (ret != OrderByResult.reverseCursor) return OrderByResult.temporaryTable;
                    } else {
                        if (ret == null) ret = OrderByResult.normal;
                        else if (ret != OrderByResult.normal) return OrderByResult.temporaryTable;
                    }
                } else {
                    if (orderInCursor.getDirection() != orderInRequest.getDirection()) {
                        return OrderByResult.temporaryTable;
                    } else {
                        if (ret == null) ret = OrderByResult.normal;
                        else if (ret != OrderByResult.normal) return OrderByResult.temporaryTable;
                    }
                }

            }

            if (ret != null) {
                return ret;
            } else {
                return OrderByResult.temporaryTable;
            }
        }
        return OrderByResult.temporaryTable;
    }

    /**
     * 只要ordersInCursor中包含所有的ordersInRequest，不论方向顺序，则不需要排序 用于distinct
     * 
     * @param ordersInCursor
     * @param ordersInRequest
     * @param executionContext
     * @return
     */
    protected static OrderByResult chooseOrderByMethodNotNeedOrderMatch(List<IOrderBy> ordersInCursor,
                                                                        List<IOrderBy> ordersInRequest,
                                                                        ExecutionContext executionContext) {
        if (ordersInRequest != null && ordersInRequest.size() <= ordersInCursor.size()) {
            // 察看order顺序
            int requestOrderSize = ordersInRequest.size();
            OrderByResult ret = null;
            for (int i = 0; i < requestOrderSize; i++) {
                IOrderBy orderInRequest = ordersInRequest.get(i);
                boolean columnNotMatch = true;
                for (int j = 0; j < ordersInCursor.size(); j++) {
                    IOrderBy orderInCursor = ordersInCursor.get(j);
                    columnNotMatch = columnNotMatch & !isTwoOrderByMatched(orderInCursor, orderInRequest);
                    if (columnNotMatch == false) {
                        // 出现false，代表有一个匹配成功
                        break;
                    }
                }

                if (columnNotMatch) {
                    return OrderByResult.temporaryTable;
                } else {
                    ret = OrderByResult.normal;
                }
            }
            if (ret != null) {
                return ret;
            } else {
                return OrderByResult.temporaryTable;
            }
        }
        return OrderByResult.temporaryTable;
    }

    /**
     * <pre>
     * 算order应该用什么方法来实现的方法。 具体可以看OrderByResult的解说 
     * 1. 如果全部列都匹配，并且asc也全部匹配，那么认为是normal. 
     * 2. 只要有一个列不匹配，那么就是临时表 
     * 3. 第三中情况略微复杂，做个详细说明
     *    用户的request里面有可能会出现几种情况 
     *    1. 用户请求排序顺序与实际数据顺序完全一致，那么应该正常返回。 
     *    2. 用户请求顺序与实际数据顺序完全相反，那么应该返回反转cursor
     *    3. 用户请求顺序与实际数据顺序出现反转后反转，返回临时表。
     * （比如用户请求:order by colA(asc),B(desc),C(asc) 真正的数据顺序 A(desc),B(asc),C(desc) 。
     * 
     * <pre>
     * @param cursor
     * @param ordersInRequest
     * @param executionContext
     * @param IQueryTree
     * @param needOrderMatch
     * 为true时，ordersInRequest和ordersInCursor必须顺序一致才会认定为不需要排序
     * 为false时，只要ordersInCursor包含ordersInRequest中的列即可
     * ，不必要顺序一致，亦不考虑order的direction
     * @return
     */
    protected static OrderByResult chooseOrderByMethod(ISchematicCursor cursor, List<IOrderBy> ordersInRequest,
                                                       ExecutionContext executionContext, boolean needOrderMatch) {
        if (cursor.getJoinOrderBys() != null && cursor.getJoinOrderBys().size() > 1) {
            OrderByResult last = OrderByResult.temporaryTable;
            for (List<IOrderBy> ordersInCursor : cursor.getJoinOrderBys()) {
                if (ordersInCursor == null) {
                    ordersInCursor = Collections.emptyList();
                }
                OrderByResult result = chooseOrderByMethod(ordersInCursor,
                    ordersInRequest,
                    executionContext,
                    needOrderMatch);
                // 不可能出现一个匹配reverse，另一个匹配normal的情况
                if (result == OrderByResult.normal) {
                    return result;
                } else if (result.ordinal() > last.ordinal()) {
                    last = result;
                }
            }
            // 没有匹配的normal/reverseCurosr，直接返回临时表
            return last;
        } else {
            List<IOrderBy> ordersInCursor = cursor.getOrderBy();
            if (ordersInCursor == null) {
                ordersInCursor = Collections.emptyList();
            }
            return chooseOrderByMethod(ordersInCursor, ordersInRequest, executionContext, needOrderMatch);
        }
    }

    protected static enum OrderByResult {
        /**
         * 临时表
         */
        temporaryTable,
        /**
         * 需要反转
         */
        reverseCursor,
        /**
         * 正常
         */
        normal;
    }

    /**
     * 用于处理列的选择性filter 如 A join B 总共有3列 A.a,A.b,B.a 实际上只需要两列。那么 这里就允许进行列的filter
     * 
     * @param cursor
     * @param executionContext
     * @param IQueryTree
     * @return
     * @throws TddlException
     */
    protected ISchematicCursor processColumnAndAlias(ISchematicCursor cursor, final ExecutionContext executionContext,
                                                     IQueryTree IQueryTree) throws TddlException {
        if (IQueryTree.getColumns() == null || IQueryTree.getColumns().isEmpty()) {
            return cursor;
        }
        IRepository repo = executionContext.getCurrentRepository();
        List<ISelectable> retColumns = IQueryTree.getColumns();
        // 过滤多其它不必要的select字段
        cursor = repo.getCursorFactory().columnAliasCursor(executionContext, cursor, retColumns, IQueryTree.getAlias());
        return cursor;
    }

    protected ISchematicCursor processValueFilter(ISchematicCursor cursor, final ExecutionContext executionContext,
                                                  IQueryTree IQueryTree) throws TddlException {
        // 接着处理valueFilter
        IFilter _valueFilter = IQueryTree.getValueFilter();
        if (_valueFilter != null) {
            cursor = executionContext.getCurrentRepository()
                .getCursorFactory()
                .valueFilterCursor(executionContext, cursor, IQueryTree.getValueFilter());
        }
        return cursor;
    }

    protected ISchematicCursor processHavingFilter(ISchematicCursor cursor, final ExecutionContext executionContext,
                                                   IQueryTree IQueryTree) throws TddlException {
        // 接着处理havingFilter
        IFilter havingFilter = IQueryTree.getHavingFilter();
        if (havingFilter != null) {
            cursor = executionContext.getCurrentRepository()
                .getCursorFactory()
                .valueFilterCursor(executionContext, cursor, IQueryTree.getHavingFilter());
        }
        return cursor;
    }

    protected ISchematicCursor processLimitFromTo(ISchematicCursor cursor, final ExecutionContext executionContext,
                                                  IQueryTree IQueryTree) throws TddlException {
        // 接着处理valueFilter
        if (IQueryTree.getLimitTo() == null || (Long) IQueryTree.getLimitTo() == 0l) {// 如果limitTo为空或为0，则意味着不需要包装limit
            // from
            // to这个东西到cursor上。
            return cursor;
        }

        if (IQueryTree.getLimitFrom() == null) {
            // should not be here...
            logger.warn("should not be here ,limit from is null .but this val should be fill with 0 before there");
            IQueryTree.setLimitFrom(0);
        }
        // limit to 不为空，limit from也不为空。所以包装个limit from To cursor
        // 如果limit from 也不
        cursor = executionContext.getCurrentRepository()
            .getCursorFactory()
            .limitFromToCursor(executionContext,
                cursor,
                (Long) IQueryTree.getLimitFrom(),
                (Long) IQueryTree.getLimitTo());
        return cursor;
    }

    /**
     * <pre>
     * 判断前缀索引 可判断o1是否包含o2.在组合索引里，要尽可能匹配更多的列。 在用于组合索引排序的时候 o1 是表的源组合索引。 o2
     * 是where条件中需要的走索引的key filters的组合关系。 
     * 假如 ： 
     * 1. 原来的组合索引(o1)是A ,B -> PK 如果o2 key filter是 A ,则返回0
     * 2. 如果o2 key filter是A,B 则返回0 ,  o2 Key Filter是C 返回-1, 无关其他属性，返回0，也就意味着不需要排序。 
     * 
     * -1 一般来说会导致使用临时表。 0不会
     * </pre>
     * 
     * @param o2 条件，参数
     * @param o1 索引本身的顺序
     * @return 0 : 完全正常匹配成功,相等 1 : 前缀匹配 -1 : 不匹配
     */
    protected final int matchIndex(List<IOrderBy> o2, List<IOrderBy> o1) {
        if (o1 == null || o2 == null) {
            return MATCH;
        }
        if (o2.isEmpty()) {
            return MATCH;
        }
        if (o1.isEmpty()) {
            return NOT_MATCH;
        }
        if (o2.size() > o1.size()) {// o2.length比o1大，返回-1，不可用
            return NOT_MATCH;
        }
        for (int i = 0; i < o2.size(); i++) {
            if (!isTwoOrderByMatched(o1.get(i), o2.get(i))) {
                return NOT_MATCH;
            }
            if (i == o2.size() - 1) {
                if (o1.size() > o2.size()) {
                    return PREFIX_MATCH;

                } else {
                    return MATCH;
                }
            }
        }
        return PREFIX_MATCH;
    }

    protected Future<ISchematicCursor> executeFuture(final ExecutionContext executionContext,
                                                     final IDataNodeExecutor query) throws TddlException {
        return ExecutorContext.getContext().getTopologyExecutor().execByExecPlanNodeFuture(query, executionContext);
    }

}
