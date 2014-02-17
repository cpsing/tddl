package com.taobao.tddl.executor.cursor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 * 所有cursor的抽象基类，以前的作用是存放了当前cursor的schema信息和order by信息。 现在只存放order by信息了。
 * schema因为可能会随着IRowSet动态变更，所以不在这里使用了。
 * @author Whisper
 */
public class SchematicCursor extends AbstractCursor implements ISchematicCursor {

    public List<IOrderBy> orderBys = Collections.emptyList();

    public SchematicCursor(Cursor cursor){
        super(cursor);
        if (cursor != null && cursor instanceof ISchematicCursor) {
            orderBys = ((ISchematicCursor) cursor).getOrderBy();
        } else {
            orderBys = Collections.emptyList();
        }
    }

    public SchematicCursor(Cursor cursor, List<IOrderBy> orderBys){
        super(cursor);
        this.orderBys = orderBys;
    }

    public SchematicCursor(Cursor cursor, ICursorMeta meta, List<IOrderBy> orderBys){
        super(cursor);
        this.orderBys = orderBys;
    }

    public void setOrderBy(List<IOrderBy> orderBys) {
        this.orderBys = orderBys;
    }

    public List<IOrderBy> getOrderBy() {
        return orderBys;
    }

    public List<List<IOrderBy>> getJoinOrderBys() {
        return Arrays.asList(orderBys);
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        return parentCursorSkipTo(key);
    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        return parentCursorSkipTo(key);
    }

    @Override
    public IRowSet current() throws TddlException {
        return parentCursorCurrent();
    }

    @Override
    public IRowSet next() throws TddlException {
        return parentCursorNext();
    }

    @Override
    public IRowSet prev() throws TddlException {
        return parentCursorPrev();
    }

    @Override
    public IRowSet first() throws TddlException {
        return parentCursorFirst();
    }

    @Override
    public void beforeFirst() throws TddlException {
        parentCursorBeforeFirst();
    }

    @Override
    public IRowSet last() throws TddlException {
        return parentCursorLast();
    }

    @Override
    public IRowSet getNextDup() throws TddlException {
        return parentCursorGetNextDup();
    }

    @Override
    public List<TddlException> close(List<TddlException> exceptions) {
        List<TddlException> ex = parentCursorClose(exceptions);
        return ex;
    }

    @Override
    public Cursor getCursor() {
        return parentCursorGetCursor();
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {
        return parentCursorMgetWithDuplicate(keys, prefixMatch, keyFilterOrValueFilter);
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws TddlException {
        return parentCursorMgetWithDuplicateList(keys, prefixMatch, keyFilterOrValueFilter);
    }

    @Override
    public String toStringWithInden(int inden) {
        if (cursor != null) {
            return cursor.toStringWithInden(inden);
        } else {
            return GeneralUtil.getTab(inden) + "SchematicCursor";
        }

    }

    @Override
    public List<ColumnMeta> getReturnColumns() throws TddlException {
        return parentCursorGetReturnColumns();
    }

}
