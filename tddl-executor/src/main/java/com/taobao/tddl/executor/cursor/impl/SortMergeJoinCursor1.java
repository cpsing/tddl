package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.codec.RecordCodec;
import com.taobao.tddl.executor.common.ICursorMeta;
import com.taobao.tddl.executor.cursor.IANDCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.rowset.JoinRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;

/**
 * @author jianxing <jianxing.qx@taobao.com> sort merge 实现
 * 假定left和right都是有序数据。将两个数据进行合并。 应该改成sort merge Cursor..
 * @author whisper
 */
@SuppressWarnings("rawtypes")
public class SortMergeJoinCursor1 extends JoinSchematicCursor implements IANDCursor {

    /**
     * 这是个比较让人困惑的东西。 是用来取下一个相同的数据的Cursor ，与nextDup并用。
     */
    protected ISchematicCursor dup_cursor;
    /**
     * 左面的key
     */
    protected CloneableRecord  left_key;
    /**
     * 右面的key
     */
    protected CloneableRecord  right_key;
    /**
     * 上一个符合要求的join以后的kv
     */
    protected IRowSet          prevJoinedKV;
    /**
     * 用于标记是否存在这下一个相同的值的right kvPair
     */
    protected boolean          nextDup;
    protected boolean          left_prefix;

    protected IRowSet          prev;

    protected IRowSet          current;
    /**
     * 右边是否使用前缀匹配。 前缀匹配，就是说，一个索引是a,b,c->pk 那么如果输入a,b 希望找到所有符合ab，但c无所谓的pk
     * list时候使用的方法。
     */
    protected boolean          right_prefix;

    // boolean dup_prefix;

    protected RecordCodec      leftCodec  = null;

    protected RecordCodec      rightCodec = null;

    @SuppressWarnings("unchecked")
    public SortMergeJoinCursor1(ISchematicCursor left_cursor, ISchematicCursor right_cursor, List left_columns,
                                List right_columns, boolean left_prefix, boolean right_prefix, List<IOrderBy> orderBys,
                                List leftColumns, List rightColumns) throws Exception{
        super(left_cursor, right_cursor, left_columns, right_columns, leftColumns, rightColumns);
        this.left_cursor = left_cursor;
        this.right_cursor = right_cursor;
        this.left_prefix = left_prefix;
        this.right_prefix = right_prefix;

        List<ColumnMeta> colMetas = ExecUtils.convertIColumnsToColumnMeta(left_columns);
        leftCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(colMetas);
        this.left_key = leftCodec.newEmptyRecord();

        colMetas = ExecUtils.convertIColumnsToColumnMeta(right_columns);
        rightCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(colMetas);
        this.right_key = rightCodec.newEmptyRecord();
    }

    // @SuppressWarnings("unchecked")
    // public SortMergeJoinCursor1(ISchematicCursor left_cursor,
    // ISchematicCursor right_cursor, List left_columns,
    // List right_columns, List columns, boolean left_prefix,
    // boolean right_prefix, List<IOrderBy> orderBys) throws Exception {
    // this(left_cursor, right_cursor, left_columns, right_columns, columns,
    // left_prefix, right_prefix, left_cursor.getOrderBy(), ExecUtil
    // .getComp(left_columns, right_columns));
    // }

    // public SortMergeJoinCursor1(ISchematicCursor left_cursor,
    // ISchematicCursor right_cursor, List left_columns,
    // List right_columns, List columns) throws Exception {
    // this(left_cursor, right_cursor, left_columns, right_columns, columns,
    // false, false);
    // }
    //
    //
    // public SortMergeJoinCursor1(ISchematicCursor left_cursor,
    // ISchematicCursor right_cursor, List left_columns,
    // List right_columns, List columns, List<IOrderBy> orderBys)
    // throws Exception {
    // this(left_cursor, right_cursor, left_columns, right_columns, columns,
    // false, false, orderBys);
    // }
    //
    // public SortMergeJoinCursor1(ISchematicCursor left_cursor,
    // ISchematicCursor right_cursor, List left_columns,
    // List right_columns, List columns, List<IOrderBy> orderBys,
    // Comparator<KVPair> kvPairComparator) throws Exception {
    // this(left_cursor, right_cursor, left_columns, right_columns, columns,
    // false, false, orderBys, kvPairComparator);
    // }

    public static List<IOrderBy> getOrderBy(List<IColumn> columns) {
        List<IOrderBy> ret = new ArrayList<IOrderBy>();
        for (IColumn c : columns) {
            ret.add(ASTNodeFactory.getInstance().createOrderBy().setColumn(c).setDirection(true));
        }
        return ret;
    }

    /**
     * TODO shenxun : 需要测试
     * 
     * @return @throws FetchException
     */
    @Override
    public IRowSet next() throws Exception {
        if (nextDup) {
            // 有相同的的，那么取一个相同的
            nextDup = false;
            return current;
        }

        if (prev != null && dup_cursor != null) {
            IRowSet dup = dup_cursor.getNextDup();
            if (dup != null) {
                return joinRecord(prev, dup);
            }
        }

        IRowSet left = left_cursor.next();
        IRowSet right = right_cursor.next();
        if (left == null || right == null) {
            // 有一边儿为空，则认为数据已经没有join的可能了
            return null;
        }
        int n = kvPairComparator.compare(left, right);
        do {
            GeneralUtil.checkInterrupted();
            if (n == 0) {
                // 相等，直接返回joinRecord
                prevJoinedKV = current;
                current = joinRecord(left, right);
                return current;
            } else if (n < 0) {
                List<Integer> rightJoinOnColumnsIndex = fillJoinOnColumnsIndex(right.getParentCursorMeta(),
                    rightJoinOnColumns);

                CloneableRecord key = get1(right, rightJoinOnColumnsIndex, leftJoinOnColumns, left_key);
                if (!left_prefix) {
                    // 非前缀索引匹配
                    if (left_cursor.skipTo(key)) {
                        left = left_cursor.current();
                        n = kvPairComparator.compare(left, right);
                        if (n == 0) {
                            prev = right;
                            dup_cursor = left_cursor;
                            // dup_prefix = false;
                        }
                    } else {
                        return null;
                    }
                } else {
                    // 前缀索引匹配。
                    // RangeCursor.RangeFilter[] rfs = getRangeFilters(
                    // rightJoinOnColumns, leftJoinOnColumns, right,
                    // left_cursor.getMeta());
                    // RangeCursor range_cursor = new RangeCursor(left_cursor,
                    // rfs);
                    // left = range_cursor.next();
                    // if (left == null) {
                    // return null;
                    // }
                    // n = kvPairComparator.compare(left, right);
                    // if (n == 0) {
                    // prev = right;
                    // dup_cursor = range_cursor;
                    // // dup_prefix = true;
                    // }
                    throw new UnsupportedOperationException();
                }
            } else {

                if (!right_prefix) {
                    List<Integer> leftJoinOnColumnsIndex = fillJoinOnColumnsIndex(left.getParentCursorMeta(),
                        leftJoinOnColumns);
                    if (right_cursor.skipTo(get1(left, leftJoinOnColumnsIndex, rightJoinOnColumns, right_key))) {
                        right = right_cursor.current();
                        n = kvPairComparator.compare(right, left);
                        if (n == 0) {
                            prev = left;
                            dup_cursor = right_cursor;
                            // dup_prefix = false;
                        }
                    } else {
                        return null;
                    }
                } else {
                    throw new UnsupportedOperationException();
                    // RangeCursor.RangeFilter[] rfs = getRangeFilters(
                    // leftJoinOnColumns, rightJoinOnColumns, left,
                    // right_cursor.getMeta());
                    // RangeCursor range_cursor = new RangeCursor(right_cursor,
                    // rfs);
                    // right = range_cursor.next();
                    // if (right == null) {
                    // return null;
                    // }
                    // n = kvPairComparator.compare(right, left);
                    // if (n == 0) {
                    // prev = left;
                    // dup_cursor = range_cursor;
                    // // dup_prefix = true;
                    // }

                }
            }

        } while (n >= 0);

        return null;
    }

    private List<Integer> fillJoinOnColumnsIndex(ICursorMeta icm, List joinOnColumns) throws IllegalStateException {
        List<Integer> joinOnColumnsIndex = new ArrayList<Integer>(joinOnColumns.size());
        for (Object oneColObj : joinOnColumns) {
            IColumn oneRightColumn = ExecUtils.getIColumn(oneColObj);
            Integer index = icm.getIndex(oneRightColumn.getTableName(), oneRightColumn.getColumnName());

            if (index == null) index = icm.getIndex(oneRightColumn.getTableName(), oneRightColumn.getAlias());
            if (index == null) {
                throw new IllegalStateException("can't find col : " + oneColObj + " can't do join operation . ");
            }
            joinOnColumnsIndex.add(index);
        }
        return joinOnColumnsIndex;
    }

    // @Override
    // public IRowSet getNextDup() throws Exception {
    // if (prevJoinedKV == null) {
    // return null;
    // }
    // nextDup = true;
    // if (next() != null) {
    // if (current.compareTo(prevJoinedKV) == 0) {
    // nextDup = false;
    // return current;
    // }
    // }
    // return null;
    // }

    // @Override
    // public boolean skipTo(KVPair kv) throws Exception {
    // return skipTo(kv.getKey());
    // }
    // 直接丢异常

    // @Override
    // public boolean skipTo(CloneableRecord key) throws Exception {
    // KVPair kv;
    // while ((kv = next()) != null) {
    // if (kv.getKey().equals(key)) {
    // current = kv;
    // return true;
    // }
    // }
    // return false;
    // }
    // 直接丢异常

    @Override
    public IRowSet current() throws Exception {
        return current;
    }

    // @Override
    // public KVPair first() throws Exception {
    // throw new UnsupportedOperationException();
    // }

    // @Override
    // public KVPair last() throws Exception {
    // throw new UnsupportedOperationException();
    // }

    // @Override
    // public KVPair prev() throws Exception {
    // throw new UnsupportedOperationException();
    // }

    /**
     * 从右面将值取出后，用左面的名字进行包装
     * 
     * @param from_kv
     * @param from_columns
     * @param to_columns
     * @param to_key
     * @return
     */
    public CloneableRecord get1(IRowSet from_kv, List<Integer> fromCololumnIndexes, List to_columns,
                                CloneableRecord to_key) {
        for (int k = 0; k < fromCololumnIndexes.size(); k++) {
            Object v = fromCololumnIndexes.get(k);
            to_key.put(ExecUtils.getColumn(to_columns.get(k)).getColumnName(), v);
        }
        return to_key;
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
        ;
        IRowSet joinedRowSet = new JoinRowSet(rightCursorOffset, kv1, kv2, joinCursorMeta);
        return joinedRowSet;
    }

    // @Override
    // public void close() throws Exception {
    // left_cursor.close();
    // right_cursor.close();
    // }
    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String subQueryTab = GeneralUtil.getTab(inden);
        ExecUtils.printMeta(joinCursorMeta, inden, sb);
        ExecUtils.printOrderBy(orderBys, inden, sb);
        sb.append(subQueryTab).append("【Sort Merge Join : ").append("\n");
        sb.append(subQueryTab).append("leftCursor:").append("\n");
        sb.append(left_cursor.toStringWithInden(inden + 1));
        sb.append(subQueryTab).append("rightCursor:").append("\n");
        sb.append(right_cursor.toStringWithInden(inden + 1));

        return sb.toString();
    }
}
