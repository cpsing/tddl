package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.codec.RecordCodec;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.IBlockNestedLoopCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.IValueFilterCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.ICursorFactory;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;

/**
 * @author mengshi <mengshi.sunmengshi@taobao.com> Block Nested Loop Join
 */
public class BlockNestedtLoopCursor extends IndexNestedLoopMgetImpCursor implements IBlockNestedLoopCursor {

    ICursorFactory      cursorFactory    = null;
    ExecutionContext    executionContext = null;
    // ICursorMeta rightCursorMeta = null;
    private final IJoin join;
    private RecordCodec leftCodec;

    public BlockNestedtLoopCursor(ISchematicCursor leftCursor, ISchematicCursor rightCursor, List leftColumns,
                                  List rightColumns, List columns, ICursorFactory cursorFactory, IJoin join,
                                  ExecutionContext executionContext, List leftRetColumns, List rightRetColumns)
                                                                                                               throws Exception{
        super(leftCursor, rightCursor, leftColumns, rightColumns, columns, leftRetColumns, rightRetColumns, join);
        this.cursorFactory = cursorFactory;
        this.leftCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
            .getCodec(ExecUtils.getColumnMetas(rightColumns));
        this.left_key = leftCodec.newEmptyRecord();
        this.executionContext = executionContext;
        // rightCursorMeta =
        // CursorMetaImp.buildNew(ExecUtils.convertISelectablesToColumnMeta(rightRetColumns,
        // join.getRightNode().getAlias(),
        // join.isSubQuery()));
        this.join = join;
    }

    public BlockNestedtLoopCursor(ISchematicCursor leftCursor, ISchematicCursor rightCursor, List leftColumns,
                                  List rightColumns, List columns, boolean prefix, ICursorFactory cursorFactory,
                                  List leftRetColumns, List rightRetColumns, IJoin join) throws Exception{
        super(leftCursor,
            rightCursor,
            leftColumns,
            rightColumns,
            columns,
            prefix,
            leftRetColumns,
            rightRetColumns,
            join);
        this.join = join;
        this.cursorFactory = cursorFactory;
    }

    protected Map<CloneableRecord, DuplicateKVPair> getRecordFromRightByValueFilter(List<CloneableRecord> leftJoinOnColumnCache)
                                                                                                                                throws TddlException {

        right_cursor.beforeFirst();
        if (isLeftOutJoin() && this.rightCursorMeta == null) {
            IRowSet kv = right_cursor.next();
            if (kv != null) {
                this.rightCursorMeta = kv.getParentCursorMeta();
            } else {
                rightCursorMeta = CursorMetaImp.buildNew(right_cursor.getReturnColumns());
            }

            right_cursor.beforeFirst();
        }

        IBooleanFilter filter = ASTNodeFactory.getInstance().createBooleanFilter();

        List<Object> values = new ArrayList<Object>();
        for (CloneableRecord record : leftJoinOnColumnCache) {
            Map<String, Object> recordMap = record.getMap();
            if (recordMap.size() != 1) {
                throw new IllegalArgumentException("目前只支持单值查询吧。。简化一点");
            }
            Comparable comp = (Comparable) record.getMap().values().iterator().next();
            values.add(comp);
        }

        filter.setOperation(OPERATION.IN);
        filter.setValues(values);
        filter.setColumn(this.rightJoinOnColumns.get(0));
        IColumn rightColumn = (IColumn) this.rightJoinOnColumns.get(0);
        IValueFilterCursor vfc = this.cursorFactory.valueFilterCursor(executionContext, right_cursor, filter);

        Map<CloneableRecord, DuplicateKVPair> records = new HashMap();

        if (isLeftOutJoin() && !isRightOutJoin()) {
            blockNestedLoopJoin(leftJoinOnColumnCache, rightColumn, vfc, records);
        } else if (!isLeftOutJoin() && !isRightOutJoin()) {
            // inner join
            blockNestedLoopJoin(leftJoinOnColumnCache, rightColumn, vfc, records);
        } else {
            throw new UnsupportedOperationException("不支持该操作  leftOutJoin:" + isLeftOutJoin() + " ; rightOutJoin:"
                                                    + isRightOutJoin());
        }
        return records;
    }

    @Override
    protected Map<CloneableRecord, DuplicateKVPair> getRecordFromRight(List<CloneableRecord> leftJoinOnColumnCache)
                                                                                                                   throws TddlException {
        // 子查询的话不能用mget
        // 因为子查询的话，join的列可以是函数，函数应该放在having里，而不是放在valueFilter里
        if (this.join.getRightNode().isSubQuery()) {
            return this.getRecordFromRightByValueFilter(leftJoinOnColumnCache);
        } else {
            return right_cursor.mgetWithDuplicate(leftJoinOnColumnCache, false, false);
        }
    }

    // private void leftOutJoin(List<CloneableRecord> leftJoinOnColumnCache,
    // IColumn rightColumn, IValueFilterCursor vfc,
    // Map<CloneableRecord, DuplicateKVPair> records) throws TddlException {
    // Map<Comparable, CloneableRecord> leftMap = new HashMap<Comparable,
    // CloneableRecord>();
    // Map<Comparable, CloneableRecord> tempMap = new HashMap<Comparable,
    // CloneableRecord>();
    // for (CloneableRecord record : leftJoinOnColumnCache) {
    // // 去重
    // Comparable comp = (Comparable)
    // record.getMap().values().iterator().next();
    // leftMap.put(comp, record);
    // tempMap.put(comp, record);
    // }
    //
    // IRowSet kv = ExecUtils.fromIRowSetToArrayRowSet(vfc.next());
    // if (kv != null) {
    // do {
    // kv = ExecUtils.fromIRowSetToArrayRowSet(kv);
    // Object rightValue = ExecUtils.getValueByIColumn(kv, rightColumn);
    // if (leftMap.containsKey(rightValue)) {
    // tempMap.remove(rightValue);
    // CloneableRecord record = leftMap.get(rightValue);
    // buildDuplicate(records, kv, record);
    // }
    // } while ((kv = vfc.next()) != null);
    // }
    // if (!tempMap.isEmpty() && !leftJoinOnColumnCache.isEmpty()) {
    // kv = new ArrayRowSet(rightCursorMeta, new
    // Object[rightCursorMeta.getColumns().size()]);
    // for (CloneableRecord record : tempMap.values()) {
    // buildDuplicate(records, kv, record);
    // }
    // }
    // }

    private void blockNestedLoopJoin(List<CloneableRecord> leftJoinOnColumnCache, IColumn rightColumn,
                                     IValueFilterCursor vfc, Map<CloneableRecord, DuplicateKVPair> records)
                                                                                                           throws TddlException {
        IRowSet kv = null;
        while ((kv = vfc.next()) != null) {

            kv = ExecUtils.fromIRowSetToArrayRowSet(kv);
            Object rightValue = ExecUtils.getValueByIColumn(kv, rightColumn);
            for (CloneableRecord record : leftJoinOnColumnCache) {
                Comparable comp = (Comparable) record.getMap().values().iterator().next();
                if (rightValue.equals(comp)) {
                    buildDuplicate(records, kv, record);
                    break;
                }
            }
        }
    }

    private void buildDuplicate(Map<CloneableRecord, DuplicateKVPair> records, IRowSet kv, CloneableRecord record) {
        DuplicateKVPair dkv = records.get(record);
        if (dkv == null) {
            dkv = new DuplicateKVPair(kv);
            records.put(record, dkv);
        } else {
            while (dkv.next != null) {
                dkv = dkv.next;
            }
            dkv.next = new DuplicateKVPair(kv);
        }
    }

}
