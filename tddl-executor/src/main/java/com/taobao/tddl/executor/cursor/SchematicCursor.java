package com.taobao.tddl.executor.cursor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.ICursorMeta;
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

    @Override
    public boolean skipTo(CloneableRecord key) throws Exception {
        return parentCursorSkipTo(key);
    }

    @Override
    public boolean skipTo(KVPair key) throws Exception {
        return parentCursorSkipTo(key);
    }

    @Override
    public IRowSet current() throws Exception {
        return parentCursorCurrent();
    }

    @Override
    public IRowSet next() throws Exception {
        return parentCursorNext();
    }

    @Override
    public IRowSet prev() throws Exception {
        return parentCursorPrev();
    }

    @Override
    public IRowSet first() throws Exception {
        return parentCursorFirst();
    }

    @Override
    public void beforeFirst() throws Exception {
        parentCursorBeforeFirst();
    }

    @Override
    public IRowSet last() throws Exception {
        return parentCursorLast();
    }

    @Override
    public IRowSet getNextDup() throws Exception {
        return parentCursorGetNextDup();
    }

    @Override
    public List<Exception> close(List<Exception> exceptions) {
        List<Exception> ex = parentCursorClose(exceptions);
        return ex;
    }

    @Override
    public Cursor getCursor() {
        return parentCursorGetCursor();
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws Exception {
        return parentCursorMgetWithDuplicate(keys, prefixMatch, keyFilterOrValueFilter);
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws Exception {
        return parentCursorMgetWithDuplicateList(keys, prefixMatch, keyFilterOrValueFilter);
    }

    @Override
    public String toStringWithInden(int inden) {

        if (cursor != null) return cursor.toStringWithInden(inden);
        return GeneralUtil.getTab(inden) + "SchematicCursor";

    }

    @Override
    public List<ColumnMeta> getReturnColumns() throws Exception {
        return parentCursorGetReturnColumns();
    }
}
