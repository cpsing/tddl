package com.taobao.tddl.executor.spi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codehaus.groovy.util.StringUtil;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ICursorMeta;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.cursor.IANDCursor;
import com.taobao.tddl.executor.cursor.IAggregateCursor;
import com.taobao.tddl.executor.cursor.IColumnAliasCursor;
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
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.cursor.impl.AffectRowCursor;
import com.taobao.tddl.executor.cursor.impl.AggregateCursor;
import com.taobao.tddl.executor.cursor.impl.BlockNestedtLoopCursor;
import com.taobao.tddl.executor.cursor.impl.ColumnAliasCursor;
import com.taobao.tddl.executor.cursor.impl.InCursor;
import com.taobao.tddl.executor.cursor.impl.IndexNestedLoopMgetImpCursor;
import com.taobao.tddl.executor.cursor.impl.LimitFromToCursor;
import com.taobao.tddl.executor.cursor.impl.MergeSortedCursors;
import com.taobao.tddl.executor.cursor.impl.RangeCursor1;
import com.taobao.tddl.executor.cursor.impl.ReverseOrderCursor;
import com.taobao.tddl.executor.cursor.impl.SetOrderByCursor;
import com.taobao.tddl.executor.cursor.impl.SortMergeJoinCursor1;
import com.taobao.tddl.executor.cursor.impl.TempTableSortCursor;
import com.taobao.tddl.executor.cursor.impl.ValueFilterCursor;
import com.taobao.tddl.optimizer.config.Group;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;

/**
 * 默认的cursor工厂
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午3:56:20
 * @since 5.1.0
 */
@SuppressWarnings("rawtypes")
public class CursorFactoryDefaultImpl implements CursorFactory {

    private AndorContext commonConfig;

    public CursorFactoryDefaultImpl(AndorContext commonConfig){
        super();
        this.commonConfig = commonConfig;
    }

    /*
     * AggregateCursor aggregateCursor(SchematicCursor cursor, List<IFunction>
     * aggregates,boolean isMerge) { return new AggregateCursor(cursor,
     * aggregates,isMerge); }
     */
    ResultCursor resultCursor(SchematicCursor cursor, String exception) {
        try {
            return new ResultCursor(cursor, exception);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

    protected void closeParentCursor(Cursor parentCursor) {
        if (parentCursor != null) {
            List<Exception> exs = new ArrayList();
            exs = parentCursor.close(exs);
            if (!exs.isEmpty()) throw new RuntimeException(GeneralUtil.mergeException(exs));

        }
    }

    ResultCursor resultCursor(SchematicCursor cursor, Map<String, Comparable> context) {
        try {
            return new ResultCursor(cursor, context);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

    @Override
    public IAggregateCursor aggregateCursor(ISchematicCursor cursor, List<IFunction> aggregates,
                                            List<IOrderBy> groupBycols, List<ISelectable> allSelectable, boolean isMerge) {
        try {
            return new AggregateCursor(cursor, aggregates, groupBycols, allSelectable, isMerge);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

    @Override
    public IColumnAliasCursor columnAliasCursor(ISchematicCursor cursor, List<ISelectable> retColumns, String name) {
        try {
            return new ColumnAliasCursor(cursor, retColumns, name);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

    @Override
    public ValueFilterCursor valueFilterCursor(ISchematicCursor cursor, IFilter filter,
                                               ExecutionContext executionContext) {
        try {
            return new ValueFilterCursor(cursor, filter, null);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

    @Override
    public IORCursor mergeSortedCursor(List<ISchematicCursor> cursors, boolean duplicated, String tableAlias)
                                                                                                             throws Exception {
        try {
            return new MergeSortedCursors(cursors, tableAlias, duplicated);
        } catch (Exception e) {
            for (ISchematicCursor cursor : cursors) {
                closeParentCursor(cursor);
            }

            throw e;
        }
    }

    @Override
    public ResultCursor resultCursor(ISchematicCursor cursor, String exception) {
        try {
            return new ResultCursor(cursor, exception);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

    @Override
    public ResultCursor resultCursor(ISchematicCursor cursor, Map<String, Comparable> context) {
        return new ResultCursor(cursor, context);
    }

    @Override
    public ITempTableSortCursor tempTableSortCursor(ISchematicCursor cursor, List<IOrderBy> orderBys,
                                                    boolean sortedDuplicates, long requestID,
                                                    Map<String, Comparable> extraContext) throws FetchException,
                                                                                         Exception {
        try {
            Comparable comp = extraContext.get(ExtraCmd.ExecutionExtraCmd.ALLOW_TEMPORARY_TABLE);
            // 只有当AllowTemporaryTable不为空，并且为true的时候，才允许使用临时表。
            if (comp != null) {
                String valueInStr = StringUtil.trim(String.valueOf(comp));
                Boolean valueInBool = Boolean.valueOf(valueInStr);
                if (valueInBool) {
                    TempTable tt = commonConfig.getOrBuildTempTable(Group.BDB_JE);
                    return new TempTableSortCursor(this, tt, cursor, orderBys, sortedDuplicates, requestID);
                }
            }
            throw new IllegalStateException("not allow to use temporary table . allow first ");

        } catch (Exception e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

    @Override
    public IReverseOrderCursor reverseOrderCursor(ISchematicCursor cursor) {
        try {
            return new ReverseOrderCursor(cursor);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

    // @Override
    // public IMergeSortCursor mergeSortCursor(ISchematicCursor cursor,
    // List<IOrderBy> orderBys, boolean prepare) throws Exception {
    // return new MergeSortCursor(cursor, orderBys);
    // }

    @Override
    public IMergeCursor mergeCursor(List<ISchematicCursor> cursors, IDataNodeExecutor currentExecotor,
                                    ExecutionContext executionContext) {
        try {
            return new MergeCursor(cursors, commonConfig, currentExecotor, executionContext);
        } catch (RuntimeException e) {
            if (cursors != null) {
                for (ISchematicCursor iSchematicCursor : cursors) {
                    closeParentCursor(iSchematicCursor);
                }
            }
            throw e;
        }
    }

    @Override
    public ResultCursor resultCursor(ISchematicCursor cursor, Map<String, Comparable> context, List<Object> retColumns) {
        try {
            return new ResultCursor(cursor, context);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

    @Override
    public IIndexNestLoopCursor indexNestLoopCursor(ISchematicCursor leftCursor, ISchematicCursor rightCursor,
                                                    List leftColumns, List rightColumns, List columns, boolean prefix,
                                                    IJoin executor) throws Exception {
        try {
            return new IndexNestedLoopMgetImpCursor(leftCursor,
                rightCursor,
                leftColumns,
                rightColumns,
                columns,
                prefix,
                executor.getLeftNode().getColumns(),
                executor.getRightNode().getColumns(),
                executor);
        } catch (Exception e) {
            closeParentCursor(leftCursor);
            closeParentCursor(rightCursor);
            throw e;
        }
    }

    @Override
    public ILimitFromToCursor limitFromToCursor(ISchematicCursor cursor, Long limitFrom, Long limitTo) {
        try {
            return new LimitFromToCursor(cursor, limitFrom, limitTo);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

    @Override
    public IMergeSortCursor join_sortMergeCursor(ISchematicCursor left_cursor, ISchematicCursor right_cursor,
                                                 List left_columns, List right_columns, List columns,
                                                 boolean left_prefix, boolean right_prefix, IJoin join)
                                                                                                       throws Exception {
        try {
            List<IOrderBy> orderBys = left_cursor.getOrderBy();
            // Comparator<IRowSet> rows=
            // ExecUtil.getComp(left_columns,right_columns,left_cursor.getMeta(),right_cursor.getMeta());
            return new SortMergeJoinCursor1(left_cursor,
                right_cursor,
                left_columns,
                right_columns,
                left_prefix,
                right_prefix,
                orderBys,
                join.getLeftNode().getColumns(),
                join.getRightNode().getColumns());
        } catch (Exception e) {
            closeParentCursor(left_cursor);
            closeParentCursor(right_cursor);
            throw e;
        }
    }

    @Override
    public AffectRowCursor affectRowCursor(int affectRow) {
        return new AffectRowCursor(affectRow);
    }

    @Override
    public ISchematicCursor schematicCursor(Cursor cursor, ICursorMeta meta, List<IOrderBy> orderBys) {
        try {
            return new SchematicCursor(cursor, meta, orderBys);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

    @Override
    public IInCursor inCursor(Cursor cursor, List<IOrderBy> orderBys, IColumn c, List<Comparable> v, OPERATION op) {
        return new InCursor(cursor, orderBys, c, v, op);
    }

    @Override
    public IMergeCursor mergeCursor(List<ISchematicCursor> cursors, ICursorMeta indexMeta,
                                    IDataNodeExecutor currentExecotor, ExecutionContext executionContext,
                                    List<IOrderBy> orderBys) {
        try {
            return new MergeCursor(cursors, indexMeta, commonConfig, currentExecotor, executionContext, orderBys);
        } catch (RuntimeException e) {
            if (cursors != null) {
                for (ISchematicCursor iSchematicCursor : cursors) {
                    closeParentCursor(iSchematicCursor);
                }
            }
            throw e;
        }
    }

    @Override
    public IANDCursor join_blockNestedLoopCursor(ISchematicCursor left_cursor, ISchematicCursor right_cursor,
                                                 List left_columns, List right_columns, List columns, IJoin join,
                                                 ExecutionContext executionContext) throws Exception {

        try {
            return new BlockNestedtLoopCursor(left_cursor,
                right_cursor,
                left_columns,
                right_columns,
                columns,
                this,
                join,
                executionContext,
                join.getLeftNode().getColumns(),
                join.getRightNode().getColumns());
        } catch (Exception e) {
            closeParentCursor(left_cursor);
            closeParentCursor(right_cursor);
            throw e;
        }
    }

    @Override
    public IRangeCursor rangeCursor(ISchematicCursor cursor, IFilter lf) {
        try {
            return new RangeCursor1(cursor, lf);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

    @Override
    public ISetOrderCursor setOrderCursor(ISchematicCursor cursor, List<IOrderBy> ordersInRequest) {
        try {
            return new SetOrderByCursor(cursor, null, ordersInRequest);
        } catch (RuntimeException e) {
            closeParentCursor(cursor);
            throw e;
        }
    }

}
