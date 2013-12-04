package com.taobao.tddl.executor.cursor.impl;

import java.util.List;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.cursor.IIndexNestLoopCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IColumn;

/**
 * @author jianxing <jianxing.qx@taobao.com> 重构用以支持mget接口实现。
 * @author whisper
 */
@SuppressWarnings("rawtypes")
public class IndexNestLoopCursor extends SortMergeJoinCursor1 implements IIndexNestLoopCursor {

    IRowSet left;
    IRowSet dup;

    protected static enum IteratorDirection {
        FORWARD, BACKWARD
    }

    private IteratorDirection iteratorDirection = null;

    /**
     * one of left join on columns
     */
    public IndexNestLoopCursor(ISchematicCursor leftCursor, ISchematicCursor rightCursor, List leftJoinOnColumns,
                               List rightJoinOnColumns, List columns, List leftColumns, List rightColumns)
                                                                                                          throws Exception{
        this(leftCursor, rightCursor, leftJoinOnColumns, rightJoinOnColumns, columns, false, leftColumns, rightColumns);
    }

    public IndexNestLoopCursor(ISchematicCursor leftCursor, ISchematicCursor rightCursor, List leftJoinOnColumns,
                               List rightJoinOnColumns, List columns, boolean prefix, List leftColumns,
                               List rightColumns) throws Exception{
        super(leftCursor,
            rightCursor,
            leftJoinOnColumns,
            rightJoinOnColumns,
            false,
            prefix,
            (leftCursor != null ? leftCursor.getOrderBy() : null),
            leftColumns,
            rightColumns);
        this.orderBys = leftCursor.getOrderBy();
    }

    @Override
    public IRowSet prev() throws Exception {
        if (iteratorDirection == null) {
            iteratorDirection = IteratorDirection.BACKWARD;
        } else if (iteratorDirection == IteratorDirection.FORWARD) {
            throw new IllegalStateException("亲，别折腾。。先前进再后退。。暂时不支持");
        }
        if (!right_prefix) {
            IRowSet pair = proecessJoinOneWithNoneProfix(false);
            return pair;
        } else {
            IRowSet pair = processJoinOneWithProfix();
            return pair;
        }
    }

    @Override
    public IRowSet next() throws Exception {
        if (iteratorDirection == null) {
            iteratorDirection = IteratorDirection.FORWARD;
        } else if (iteratorDirection == IteratorDirection.BACKWARD) {
            throw new IllegalStateException("亲，别折腾。。先前进再后退。。暂时不支持");
        }
        /*
         * 左边的cursor next一次。然后右边的next一次拼装，如果有重复key的情况。
         * 那么标记重复key为dup,利用重复key再次构建一个cursor。
         */
        if (!right_prefix) {
            IRowSet pair = proecessJoinOneWithNoneProfix(true);
            return pair;
        } else {
            IRowSet pair = processJoinOneWithProfix();
            return pair;
        }
    }

    private IRowSet processJoinOneWithProfix() throws Exception, InterruptedException {
        IRowSet ret = null;

        if (dup != null) {
            ret = processDuplicateValue();
            return ret;
        }

        throw new UnsupportedOperationException();
    }

    @Override
    public void beforeFirst() throws Exception {
        left_cursor.beforeFirst();
    }

    protected IRowSet proecessJoinOneWithNoneProfix(boolean forward) throws Exception, InterruptedException {
        IRowSet ret = null;

        if (dup != null) {
            ret = processDuplicateValue();
            return ret;
        }
        while (getOneLeftCursor(forward) != null) {
            GeneralUtil.checkInterrupted();
            putLeftCursorValueIntoReturnVal();
            boolean hasVal = right_cursor.skipTo(left_key);
            if (hasVal) {
                IRowSet right = right_cursor.current();
                ret = joinRecord(left, right);
                dup = right_cursor.getNextDup();
                dup_cursor = right_cursor;
                return ret;
            }
        }
        return null;
    }

    protected IRowSet getOneLeftCursor(boolean forward) throws Exception {
        if (forward) left = ExecUtils.fromIRowSetToArrayRowSet(left_cursor.next());
        else left = ExecUtils.fromIRowSetToArrayRowSet(left_cursor.prev());
        return left;
    }

    /**
     * 将左面的Join on columns找到。 放到右边key这个对象里。
     */
    protected void putLeftCursorValueIntoReturnVal() {
        right_key = rightCodec.newEmptyRecord();
        for (int k = 0; k < leftJoinOnColumns.size(); k++) {

            Object v = ExecUtils.getValueByIColumn(left, (IColumn) leftJoinOnColumns.get(k));

            right_key.put(ExecUtils.getColumn(rightJoinOnColumns.get(k)).getColumnName(), v);
        }
    }

    /**
     * 处理右值重复用。 左值不变，右值因为有相同key的值，所以取右，下移指针一次。
     * 
     * @return
     * @throws Exception
     */
    protected IRowSet processDuplicateValue() throws Exception {
        IRowSet ret;
        ret = joinRecord(left, dup);
        if (!right_prefix) {
            dup = dup_cursor.getNextDup();
        } else {
            dup = dup_cursor.next();
        }
        return ret;
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    @Override
    public String toStringWithInden(int inden) {

        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();

        GeneralUtil.printlnToStringBuilder(sb, tabTittle + "IndexNestedLoopCursor ");

        GeneralUtil.printAFieldToStringBuilder(sb, "leftColumns", this.leftJoinOnColumns, tabContent);

        GeneralUtil.printAFieldToStringBuilder(sb, "rightColumns", this.rightJoinOnColumns, tabContent);

        GeneralUtil.printlnToStringBuilder(sb, tabContent + "left:");
        sb.append(this.left_cursor.toStringWithInden(inden + 1));
        GeneralUtil.printlnToStringBuilder(sb, tabContent + "right:");
        sb.append(this.right_cursor.toStringWithInden(inden + 1));

        return sb.toString();
    }

    // public Map<CloneableRecord, DuplicateKVPair>
    // mgetWithDuplicate(List<CloneableRecord> keys,boolean prefixMatch,boolean
    // keyFilterOrValueFilter) throws Exception
    // {
    // this.beforeFirst();
    // IBooleanFilter filter = new PBBooleanFilterAdapter();
    //
    // List<Comparable> values = new ArrayList<Comparable>();
    // for (CloneableRecord record : keys) {
    // Map<String, Object> recordMap = record.getMap();
    // if (recordMap.size() != 1) {
    // throw new IllegalArgumentException("目前只支持单值查询吧。。简化一点");
    // }
    // Comparable comp = (Comparable) record.getMap().values().iterator()
    // .next();
    // values.add(comp);
    // }
    //
    // filter.setOperation(OPERATION.IN);
    // filter.setValues(values);
    // filter.setColumn(this.rightJoinOnColumns.get(0));
    // IColumn rightColumn = (IColumn) this.rightJoinOnColumns.get(0);
    // IValueFilterCursor vfc = this.cursorFactory.valueFilterCursor(
    // right_cursor, filter,executionContext);
    //
    // Map<CloneableRecord, DuplicateKVPair> records = new HashMap();
    //
    //
    // return records;
    // }

}
