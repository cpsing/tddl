package com.taobao.tddl.executor.cursor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;

/**
 * @author mengshi.sunmengshi 2013-11-29 下午2:39:33
 * @since 5.0.0
 */
public abstract class AbstractCursor implements Cursor {

    public Cursor     cursor;

    protected boolean inited = false;

    protected void init() throws TddlException {
        inited = true;
    }

    protected void checkInited() throws TddlException {
        if (!inited) {
            throw new RuntimeException("not yet inited ");
        }
    }

    public AbstractCursor(Cursor cursor){
        this.cursor = cursor;
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        throw new IllegalArgumentException("null object");
    }

    protected boolean parentCursorSkipTo(CloneableRecord key) throws TddlException {
        return cursor.skipTo(key);
    }

    // @Override
    // public KVPair get(KVPair key) throws TddlException {
    //
    // return cursor.get(key);
    // }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        throw new IllegalArgumentException("null object");
    }

    protected boolean parentCursorSkipTo(KVPair key) throws TddlException {
        return cursor.skipTo(key);
    }

    @Override
    public IRowSet current() throws TddlException {
        throw new IllegalArgumentException("null object");
    }

    protected IRowSet parentCursorCurrent() throws TddlException {
        return cursor.current();
    }

    @Override
    public IRowSet next() throws TddlException {
        GeneralUtil.checkInterrupted();
        throw new IllegalArgumentException("null object");
    }

    protected IRowSet parentCursorNext() throws TddlException {
        GeneralUtil.checkInterrupted();
        if (cursor == null) {
            return null;
        }
        return cursor.next();
    }

    @Override
    public IRowSet prev() throws TddlException {
        throw new IllegalArgumentException("null object");
    }

    protected IRowSet parentCursorPrev() throws TddlException {
        return cursor.prev();
    }

    @Override
    public IRowSet first() throws TddlException {
        throw new IllegalArgumentException("null object");
    }

    protected IRowSet parentCursorFirst() throws TddlException {
        return cursor.first();
    }

    @Override
    public void beforeFirst() throws TddlException {
        throw new IllegalArgumentException("null object");
    }

    protected void parentCursorBeforeFirst() throws TddlException {
        cursor.beforeFirst();
    }

    @Override
    public IRowSet last() throws TddlException {
        throw new IllegalArgumentException("null object");
    }

    protected IRowSet parentCursorLast() throws TddlException {
        return cursor.last();
    }

    //
    // @Override
    // public KVPair get(CloneableRecord key) throws TddlException {
    //
    // return cursor.get(key);
    // }

    @Override
    public IRowSet getNextDup() throws TddlException {
        throw new IllegalArgumentException("null object");
    }

    protected IRowSet parentCursorGetNextDup() throws TddlException {
        return cursor.getNextDup();
    }

    @Override
    public List<TddlException> close(List<TddlException> exceptions) {
        List<TddlException> ex = parentCursorClose(exceptions);
        return ex;
    }

    protected List<TddlException> parentCursorClose(List<TddlException> exceptions) {
        if (cursor != null) {
            List<TddlException> ex = cursor.close(exceptions);
            cursor = null;
            return ex;
        }

        if (exceptions == null) {
            exceptions = new ArrayList<TddlException>();
        }
        return exceptions;

    }

    @Override
    public boolean delete() throws TddlException {
        throw new IllegalArgumentException("null object");
    }

    protected boolean parentCursorDelete() throws TddlException {
        return cursor.delete();
    }

    @Override
    public void put(CloneableRecord key, CloneableRecord value) throws TddlException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Cursor getCursor() {
        throw new IllegalArgumentException("null object");
    }

    protected Cursor parentCursorGetCursor() {
        return cursor;
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {
        throw new IllegalArgumentException("null object");
    }

    protected Map<CloneableRecord, DuplicateKVPair> parentCursorMgetWithDuplicate(List<CloneableRecord> keys,
                                                                                  boolean prefixMatch,
                                                                                  boolean keyFilterOrValueFilter)
                                                                                                                 throws TddlException {
        return cursor.mgetWithDuplicate(keys, prefixMatch, keyFilterOrValueFilter);
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws TddlException {
        throw new IllegalArgumentException("null object");
    }

    protected List<DuplicateKVPair> parentCursorMgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                                      boolean keyFilterOrValueFilter)
                                                                                                     throws TddlException {
        return cursor.mgetWithDuplicateList(keys, prefixMatch, keyFilterOrValueFilter);
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = GeneralUtil.getTab(inden);
        GeneralUtil.printlnToStringBuilder(sb, tabTittle + "abstract cursor");
        return sb.toString();
    }

    @Override
    public boolean isDone() {
        return cursor.isDone();
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    public List<ColumnMeta> parentCursorGetReturnColumns() throws TddlException {
        return this.cursor.getReturnColumns();
    }
}
