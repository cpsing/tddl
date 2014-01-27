package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.rowset.JoinRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;

/**
 * join的结果
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午10:56:08
 * @since 5.0.0
 */
public class JoinSchematicCursor extends SchematicCursor {

    protected ISchematicCursor    left_cursor;
    protected ISchematicCursor    right_cursor;
    /**
     * 因为左右cursor要拼到一起，所以右值必须加一个偏移量
     */
    protected Integer             rightCursorOffset;
    private boolean               schemaInited   = false;
    protected ICursorMeta         joinCursorMeta = null;
    protected Comparator<IRowSet> kvPairComparator;
    protected List<ISelectable>   leftJoinOnColumns;
    protected List<ISelectable>   rightJoinOnColumns;

    /**
     * <pre>
     * 见 com.taobao.tddl.optimizer.core.ast.query.JoinNode
     * leftOuterJoin:
     *      leftOuter=true && rightOuter=false
     * rightOuterJoin:
     *      leftOuter=false && rightOuter=true
     * innerJoin:
     *      leftOuter=false && rightOuter=false 
     * outerJoin:
     *      leftOuter=true && rightOuter=true
     * </pre>
     */
    protected boolean             leftOutJoin    = false;
    protected boolean             rightOutJoin   = false;
    private List<ColumnMeta>      returnColumns;

    public JoinSchematicCursor(ISchematicCursor left_cursor, ISchematicCursor right_cursor, List leftJoinOnColumns,
                               List rightJoinOnColumns){
        super(null, null, null);
        this.left_cursor = left_cursor;
        this.right_cursor = right_cursor;

        this.leftJoinOnColumns = leftJoinOnColumns;
        this.rightJoinOnColumns = rightJoinOnColumns;

        schemaInited = false;
    }

    public void setLeftJoin(boolean left) {
        this.leftOutJoin = left;
    }

    public void setRightJoin(boolean right) {
        this.rightOutJoin = right;
    }

    public void setLeftRightJoin(IJoin join) {
        if (join != null) {
            this.leftOutJoin = join.getLeftOuter();
            this.rightOutJoin = join.getRightOuter();
        } else {
            throw new RuntimeException("IJoin join is null");
        }
    }

    public boolean isLeftOutJoin() {
        return leftOutJoin;
    }

    public boolean isRightOutJoin() {
        return rightOutJoin;
    }

    protected void buildSchemaInJoin(ICursorMeta leftCursorMeta, ICursorMeta rightCursorMeta) {
        if (schemaInited) {
            return;
        }

        schemaInited = true;
        // 以左面数据顺序，作为排序
        setOrderBy(left_cursor);

        List<ColumnMeta> leftColumns = leftCursorMeta.getColumns();
        List<ColumnMeta> rightColumns = rightCursorMeta.getColumns();
        this.kvPairComparator = ExecUtils.getComp(this.leftJoinOnColumns,
            this.rightJoinOnColumns,
            leftCursorMeta,
            rightCursorMeta);
        List<ColumnMeta> newJoinColumnMsg = new ArrayList<ColumnMeta>(leftColumns.size() + rightColumns.size());
        rightCursorOffset = leftCursorMeta.getIndexRange();
        newJoinColumnMsg.addAll(leftColumns);
        newJoinColumnMsg.addAll(rightColumns);
        List<Integer> indexes = new ArrayList<Integer>(newJoinColumnMsg.size());
        addIndexToNewIndexes(leftCursorMeta, leftColumns, indexes, 0);
        addIndexToNewIndexes(rightCursorMeta, rightColumns, indexes, rightCursorOffset);
        ICursorMeta cursorMetaImpJoin = CursorMetaImp.buildNew(newJoinColumnMsg,
            indexes,
            (leftCursorMeta.getIndexRange() + rightCursorMeta.getIndexRange()));
        // setMeta(cursorMetaImpJoin);

        this.joinCursorMeta = cursorMetaImpJoin;

    }

    protected void buildSchemaFromReturnColumns(List<ColumnMeta> leftColumns, List<ColumnMeta> rightColumns) {
        if (schemaInited) {
            return;
        }

        schemaInited = true;
        // 以左面数据顺序，作为排序
        setOrderBy(left_cursor);

        List<ColumnMeta> newJoinColumnMsg = new ArrayList<ColumnMeta>(leftColumns.size() + rightColumns.size());

        newJoinColumnMsg.addAll(leftColumns);
        newJoinColumnMsg.addAll(rightColumns);

        ICursorMeta cursorMetaImpJoin = CursorMetaImp.buildNew(newJoinColumnMsg);

        rightCursorOffset = leftColumns.size();
        this.joinCursorMeta = cursorMetaImpJoin;
    }

    private void addIndexToNewIndexes(ICursorMeta cursorMeta, List<ColumnMeta> columns, List<Integer> indexes,
                                      int offset) {
        for (ColumnMeta cm : columns) {
            Integer index = cursorMeta.getIndex(cm.getTableName(), cm.getName());
            if (index == null) {
                index = cursorMeta.getIndex(cm.getTableName(), cm.getAlias());
            }
            indexes.add(offset + index);
        }
    }

    public IRowSet joinRecord(IRowSet kv1, IRowSet kv2) {
        ICursorMeta leftCursorMeta = null;
        ICursorMeta rightCursorMeta = null;
        if (kv1 != null) {
            leftCursorMeta = kv1.getParentCursorMeta();
        }
        if (kv2 != null) {
            rightCursorMeta = kv2.getParentCursorMeta();
        }
        buildSchemaInJoin(leftCursorMeta, rightCursorMeta);

        IRowSet joinedRowSet = new JoinRowSet(rightCursorOffset, kv1, kv2, joinCursorMeta);
        return joinedRowSet;
    }

    private void setOrderBy(ISchematicCursor left_cursor) {
        List<IOrderBy> orderBys = left_cursor.getOrderBy();
        setOrderBy(orderBys);
    }

    @Override
    public List<TddlException> close(List<TddlException> exs) {
        if (left_cursor != null) {
            exs = left_cursor.close(exs);
        }
        if (right_cursor != null) {
            exs = right_cursor.close(exs);
        }

        return exs;
    }

    @Override
    public List<ColumnMeta> getReturnColumns() throws TddlException {
        if (this.returnColumns != null) {
            return this.returnColumns;
        }

        List<ColumnMeta> leftColumns = this.left_cursor.getReturnColumns();
        List<ColumnMeta> rightColumns = this.right_cursor.getReturnColumns();

        returnColumns = new ArrayList(leftColumns.size() + rightColumns.size());
        returnColumns.addAll(leftColumns);
        returnColumns.addAll(rightColumns);

        return returnColumns;

    }
}
