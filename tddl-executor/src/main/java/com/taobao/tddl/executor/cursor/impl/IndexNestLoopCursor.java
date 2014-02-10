package com.taobao.tddl.executor.cursor.impl;

import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.codec.RecordCodec;
import com.taobao.tddl.executor.cursor.IIndexNestLoopCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;

/**
 * @author jianxing <jianxing.qx@taobao.com> 重构用以支持mget接口实现。
 * @author whisper
 */
@SuppressWarnings("rawtypes")
public class IndexNestLoopCursor extends JoinSchematicCursor implements IIndexNestLoopCursor {

    protected static enum IteratorDirection {
        FORWARD, BACKWARD
    }

    protected IRowSet           left;

    protected IRowSet           dup;
    protected IRowSet           current;
    protected IteratorDirection iteratorDirection = null;
    protected boolean           right_prefix;
    protected ISchematicCursor  dup_cursor;
    protected CloneableRecord   right_key;
    protected RecordCodec       rightJoinOnColumnCodec;
    protected CloneableRecord   left_key;

    public IndexNestLoopCursor(ISchematicCursor leftCursor, ISchematicCursor rightCursor, List leftJoinOnColumns,
                               List rightJoinOnColumns, List columns, boolean prefix, List leftColumns,
                               List rightColumns) throws TddlException{

        super(leftCursor, rightCursor, leftJoinOnColumns, rightJoinOnColumns);
        this.right_prefix = prefix;
        this.orderBys = leftCursor.getOrderBy();

        this.left_cursor = leftCursor;
        this.right_cursor = rightCursor;

        List<ColumnMeta> colMetas = ExecUtils.convertIColumnsToColumnMeta(rightJoinOnColumns);
        rightJoinOnColumnCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(colMetas);
        this.right_key = rightJoinOnColumnCodec.newEmptyRecord();
    }

    /**
     * one of left join on columns
     */
    public IndexNestLoopCursor(ISchematicCursor leftCursor, ISchematicCursor rightCursor, List leftJoinOnColumns,
                               List rightJoinOnColumns, List columns, List leftColumns, List rightColumns)
                                                                                                          throws TddlException{
        this(leftCursor, rightCursor, leftJoinOnColumns, rightJoinOnColumns, columns, false, leftColumns, rightColumns);
    }

    @Override
    public void beforeFirst() throws TddlException {
        left_cursor.beforeFirst();
    }

    @Override
    public IRowSet current() throws TddlException {
        return current;
    }

    protected IRowSet getOneLeftCursor(boolean forward) throws TddlException {
        if (forward) {
            left = ExecUtils.fromIRowSetToArrayRowSet(left_cursor.next());
        } else {
            left = ExecUtils.fromIRowSetToArrayRowSet(left_cursor.prev());
        }
        return left;
    }

    @Override
    public IRowSet next() throws TddlException {
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
            current = pair;
            return pair;
        } else {
            IRowSet pair = processJoinOneWithProfix();
            current = pair;
            return pair;
        }
    }

    @Override
    public IRowSet prev() throws TddlException {
        if (iteratorDirection == null) {
            iteratorDirection = IteratorDirection.BACKWARD;
        } else if (iteratorDirection == IteratorDirection.FORWARD) {
            throw new IllegalStateException("亲，别折腾。。先前进再后退。。暂时不支持");
        }
        if (!right_prefix) {
            IRowSet pair = proecessJoinOneWithNoneProfix(false);
            current = pair;
            return pair;
        } else {
            IRowSet pair = processJoinOneWithProfix();
            current = pair;
            return pair;
        }
    }

    /**
     * 处理右值重复用。 左值不变，右值因为有相同key的值，所以取右，下移指针一次。
     * 
     * @return
     * @throws TddlException
     */
    protected IRowSet processDuplicateValue() throws TddlException {
        IRowSet ret;
        ret = joinRecord(left, dup);
        if (!right_prefix) {
            dup = dup_cursor.getNextDup();
        } else {
            dup = dup_cursor.next();
        }
        return ret;
    }

    protected IRowSet processJoinOneWithProfix() throws TddlException {
        IRowSet ret = null;

        if (dup != null) {
            ret = processDuplicateValue();
            return ret;
        }

        throw new UnsupportedOperationException();
    }

    protected IRowSet proecessJoinOneWithNoneProfix(boolean forward) throws TddlException {
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

    /**
     * 将左面的Join on columns找到。 放到右边key这个对象里。
     */
    protected void putLeftCursorValueIntoReturnVal() {
        right_key = rightJoinOnColumnCodec.newEmptyRecord();
        for (int k = 0; k < leftJoinOnColumns.size(); k++) {
            Object v = ExecUtils.getValueByIColumn(left, leftJoinOnColumns.get(k));
            right_key.put(ExecUtils.getColumn(rightJoinOnColumns.get(k)).getColumnName(), v);
        }
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

}
