package com.taobao.tddl.executor.cursor.impl;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;

/**
 * @author mengshi.sunmengshi 2013-12-3 下午2:50:43
 * @since 5.0.0
 */
public class ValueMappingCursor extends SchematicCursor {

    public ValueMappingCursor(Cursor cursor, ICursorMeta meta,
                              Map<Integer/* 返回列中的index位置 */, Integer/* 实际数据中的index位置 */> mapping){
        super(cursor);
        this.mapping = mapping;
        this.meta = meta;
    }

    Map<Integer/* 返回列中的index位置 */, Integer/* 实际数据中的index位置 */> mapping;
    ICursorMeta                                                meta = null;

    @Override
    public IRowSet current() throws TddlException {
        return wrap(super.current());
    }

    @Override
    public IRowSet next() throws TddlException {
        return wrap(super.next());
    }

    public static IRowSet wrap(ICursorMeta newCursorMeta, IRowSet target,
                               Map<Integer/* 返回列中的index位置 */, Integer/* 实际数据中的index位置 */> mapping) {
        if (target == null) {
            return null;
        }
        return new ValueMappingRowSet(newCursorMeta, target, mapping);
    }

    public IRowSet wrap(IRowSet target) {
        return wrap(meta, target, mapping);
    }

    @Override
    public IRowSet prev() throws TddlException {
        return wrap(super.prev());
    }

    @Override
    public IRowSet first() throws TddlException {
        return wrap(super.first());
    }

    @Override
    public IRowSet last() throws TddlException {
        return wrap(super.last());
    }

    @Override
    public IRowSet getNextDup() throws TddlException {
        return wrap(super.getNextDup());
    }

    @Override
    public Cursor getCursor() {
        return super.getCursor();
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {
        return super.mgetWithDuplicate(keys, prefixMatch, keyFilterOrValueFilter);
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws TddlException {
        return super.mgetWithDuplicateList(keys, prefixMatch, keyFilterOrValueFilter);
    }

}
