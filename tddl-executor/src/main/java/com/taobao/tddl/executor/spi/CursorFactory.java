package com.taobao.tddl.executor.spi;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.cursor.IANDCursor;
import com.taobao.tddl.executor.cursor.IANDDupValuesCursor;
import com.taobao.tddl.executor.cursor.IAffectRowCursor;
import com.taobao.tddl.executor.cursor.IAggregateCursor;
import com.taobao.tddl.executor.cursor.IColumnAliasCursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.IInCursor;
import com.taobao.tddl.executor.cursor.IIndexNestLoopCursor;
import com.taobao.tddl.executor.cursor.ILimitFromToCursor;
import com.taobao.tddl.executor.cursor.IMergeCursor;
import com.taobao.tddl.executor.cursor.IMergeSortCursor;
import com.taobao.tddl.executor.cursor.IORCursor;
import com.taobao.tddl.executor.cursor.IRangeCursor;
import com.taobao.tddl.executor.cursor.IReverseOrderCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.ISetOrderCursor;
import com.taobao.tddl.executor.cursor.ITempTableSortCursor;
import com.taobao.tddl.executor.cursor.IValueFilterCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;

/**
 * 这个接口的作用，就是用来进行各种基于cursor的转换处理的。 比如，如果sql中出现了alias
 * 那么这里就会有个aliasCursor做对应转换关系的这个逻辑。
 * 这层接口的作用在于，在优化时，可以复写这些实现，从而能够做到可以按照自己的存储特点，对特定查询进行优化的目的。
 * 
 * @author whisper
 */
public interface CursorFactory {

    /**
     * 用来处理合并的cursor . 对应QueryNode里面的Merge node.
     * 
     * @param cursors
     * @param orderBys
     * @return
     */
    IMergeCursor mergeCursor(List<ISchematicCursor> cursors, ICursorMeta indexMeta, IDataNodeExecutor currentExecotor,
                             ExecutionContext executionContext, List<IOrderBy> orderBys);

    /**
     * 用来处理合并的cursor . 对应QueryNode里面的Merge node.
     * 和上面的方法不同点在于，他会自动从cursors里面取第一个，然后取他的indexMeta
     * 
     * @param cursors
     * @return
     */
    IMergeCursor mergeCursor(List<ISchematicCursor> cursors, IDataNodeExecutor currentExecotor,
                             ExecutionContext executionContext);

    /**
     * 用于处理count max min avg 等函数的cursor
     * 
     * @param cursor
     * @param aggregates
     * @param groupBycols
     * @param isMerge
     * @return
     */
    IAggregateCursor aggregateCursor(ISchematicCursor cursor, List<IFunction> aggregates, List<IOrderBy> groupBycols,
                                     List<ISelectable> retColumns, boolean isMerge);

    /**
     * @param cursor
     * @param retColumns 选择列
     * @param name 表别名
     * @return
     */
    IColumnAliasCursor columnAliasCursor(ISchematicCursor cursor, List<ISelectable> retColumns, String name);

    /**
     * term-doc_id_list 合并可重复tree节点的值列表。
     * 
     * @param cursors
     * @return
     */
    IANDDupValuesCursor andDupValuesCursor(IRangeCursor... cursors);

    /**
     * @param cursorsss
     * @return
     */
    IANDDupValuesCursor andDupValuesCursor(List<IRangeCursor> cursorsss);

    /**
     * 用来针对每一个值进行过滤的cursor. 将join merge query得到的结果中的每一行，放入这个value
     * cursor里面进行匹配，为true则认为成功。
     * 
     * @param cursor
     * @param filter
     * @param executionContext TODO
     * @return
     */
    IValueFilterCursor valueFilterCursor(ISchematicCursor cursor, IFilter filter, ExecutionContext executionContext);

    /**
     * 将两组已经按照asc排序的数据，做归并的cursor 一般用于将多个子有序结果集内数据排序。
     * 
     * @param left_cursor
     * @param right_cursor
     * @return
     * @throws Exception
     */
    IORCursor mergeSortedCursor(ISchematicCursor left_cursor, ISchematicCursor right_cursor) throws Exception;

    /**
     * 最基本的cursor对象，用于给指定的cursor赋予对应的schema描述之用。
     * 
     * @param cursor
     * @param meta
     * @param orderBys
     * @return
     */
    ISchematicCursor schematicCursor(Cursor cursor, ICursorMeta meta, List<IOrderBy> orderBys);

    IAffectRowCursor affectRowCursor(int affectRow);

    /**
     * 结果集对象，封装结果集对象，用于网络传输
     * 
     * @param cursor
     * @param exception
     * @return
     */
    ResultCursor resultCursor(ISchematicCursor cursor, String exception);

    /**
     * 结果集对象，封装结果集对象，用于网络传输
     * 
     * @param cursor
     * @param context
     * @return
     */
    ResultCursor resultCursor(ISchematicCursor cursor, Map<String, Comparable> context);

    /**
     * 结果集对象，封装结果集对象，用于网络传输
     * 
     * @param cursor
     * @param context
     * @param retColumns
     * @return
     */
    ResultCursor resultCursor(ISchematicCursor cursor, Map<String, Comparable> context, List<Object> retColumns);

    /**
     * 临时表的排序用cursor ，将数据拿出写入临时表中，并进行排序。
     * 
     * @param cursor
     * @param orderBys
     * @param sortedDuplicates
     * @param extraContext TODO
     * @return
     * @throws FetchException
     * @throws Exception
     */
    ITempTableSortCursor tempTableSortCursor(ISchematicCursor cursor, List<IOrderBy> orderBys,
                                             boolean sortedDuplicates, long requestID,
                                             Map<String, Comparable> extraContext) throws Exception;

    /**
     * 对应执行计划join节点 假定右表有序，以左表的每一个值去和右表进行 join.
     * 
     * @param left_cursor
     * @param right_cursor
     * @param left_columns
     * @param right_columns
     * @param columns
     * @return
     * @throws Exception
     */
    IMergeSortCursor join_sortMergeCursor(ISchematicCursor left_cursor, ISchematicCursor right_cursor,
                                          List left_columns, List right_columns, List columns, boolean left_prefix,
                                          boolean right_prefix, IJoin joinNOde) throws Exception;

    /**
     * join的Block Nested Loop实现
     * 
     * @param left_cursor
     * @param right_cursor
     * @param left_columns
     * @param right_columns
     * @param columns
     * @param join
     * @param executionContext TODO
     * @return
     * @throws Exception
     */
    public IANDCursor join_blockNestedLoopCursor(ISchematicCursor left_cursor, ISchematicCursor right_cursor,
                                                 List left_columns, List right_columns, List columns, IJoin join,
                                                 ExecutionContext executionContext) throws Exception;

    /**
     * 如果order by col中的列，不是数据库的正常排序列，那么这个cursor会将数据查询进行颠倒操作。
     * 
     * @param cursor
     * @return
     */
    IReverseOrderCursor reverseOrderCursor(ISchematicCursor cursor);

    /**
     * 范围查询cursor . 对于key filter来说，这个cursor可以进行范围查询。 用于处理
     * 索引的key查询里面的and和or条件查找。与andCursor和orCursor不同的地方在于。
     * 这个cursor用来处理能够进行二分查找的查询的。对应keyFilter。
     * 
     * @param cursor
     * @param rangeFilters
     * @return
     */
    IRangeCursor rangeCursor(ISchematicCursor cursor, IFilter lf) throws Exception;

    /**
     * 默认右边有序，左面无序的join查询时，会调用这个cursor.一般来说，主要的用例是二级索引，
     * 所有二级索引的回表操作都是使用indexNestLoop完成的
     * 
     * @param leftCursor
     * @param rightCursor
     * @param leftColumns
     * @param rightColumns
     * @param columns
     * @return
     * @throws Exception
     */
    IIndexNestLoopCursor indexNestLoopCursor(ISchematicCursor leftCursor, ISchematicCursor rightCursor,
                                             List leftColumns, List rightColumns, List columns, boolean prefix,
                                             IJoin executor) throws Exception;

    /**
     * 从哪个值开始取，取多少个。
     * 
     * @param cursor
     * @param limitFrom
     * @param limitTo
     * @return
     */
    ILimitFromToCursor limitFromToCursor(ISchematicCursor cursor, Long limitFrom, Long limitTo);

    /**
     * id in 的优化。 会尽可能自动的将数据做分隔，比如有一组值 {0,1,2,3,4} 按照id % 2 切分的数据，那么得到的是 0 -> 0
     * , 2 , 4 1 -> 1 , 3 这样可以减少查到空值的情况，提升性能
     * 
     * @param cursor
     * @param orderBys
     * @param c
     * @param v
     * @param op
     * @return
     */
    IInCursor inCursor(Cursor cursor, List<IOrderBy> orderBys, IColumn c, List<Comparable> v, OPERATION op);

    /**
     * 合并排序后的数据用的cursor 。
     * 
     * @param left_cursor
     * @param right_cursor
     * @param duplicated
     * @return
     * @throws Exception
     */
    IORCursor mergeSortedCursor(ISchematicCursor left_cursor, ISchematicCursor right_cursor, boolean duplicated)
                                                                                                                throws Exception;

    /**
     * set request order by when cursor's orderBy tableName is not equals
     * request orderBy tableName
     * 
     * @param cursor
     * @param ordersInRequest
     * @return
     */
    ISetOrderCursor setOrderCursor(ISchematicCursor cursor, List<IOrderBy> ordersInRequest);

    IORCursor mergeSortedCursor(List<ISchematicCursor> cursors, boolean duplicated, String tableAlias) throws Exception;
}
