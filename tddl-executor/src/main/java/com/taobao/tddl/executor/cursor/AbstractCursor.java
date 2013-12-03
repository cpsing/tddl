package com.taobao.tddl.executor.cursor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.CloneableRecord;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.IRowSet;

/**
 * @author mengshi.sunmengshi 2013-11-29 下午2:39:33
 * @since 5.1.0
 */
public abstract class AbstractCursor implements Cursor {

    public Cursor     cursor;

    protected boolean inited = false;

    protected void init() throws Exception {
        inited = true;

    }

    protected void checkInited() throws Exception {
        if (!inited) {
            throw new RuntimeException("not yet inited ");
        }
    }

    public AbstractCursor(Cursor cursor){
        this.cursor = cursor;
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws Exception {
        throw new IllegalArgumentException("null object");
    }

    protected boolean parentCursorSkipTo(CloneableRecord key) throws Exception {
        return cursor.skipTo(key);
    }

    // @Override
    // public KVPair get(KVPair key) throws Exception {
    //
    // return cursor.get(key);
    // }

    @Override
    public boolean skipTo(KVPair key) throws Exception {
        throw new IllegalArgumentException("null object");
    }

    protected boolean parentCursorSkipTo(KVPair key) throws Exception {
        return cursor.skipTo(key);
    }

    @Override
    public IRowSet current() throws Exception {
        throw new IllegalArgumentException("null object");
    }

    protected IRowSet parentCursorCurrent() throws Exception {
        return cursor.current();
    }

    @Override
    public IRowSet next() throws Exception {
        GeneralUtil.checkInterrupted();
        throw new IllegalArgumentException("null object");
        // if(cursor == null) return null;
        // return cursor.next();
    }

    protected IRowSet parentCursorNext() throws Exception {
        GeneralUtil.checkInterrupted();
        if (cursor == null) return null;
        return cursor.next();
    }

    @Override
    public IRowSet prev() throws Exception {
        throw new IllegalArgumentException("null object");
    }

    protected IRowSet parentCursorPrev() throws Exception {
        return cursor.prev();
    }

    @Override
    public IRowSet first() throws Exception {
        throw new IllegalArgumentException("null object");
    }

    protected IRowSet parentCursorFirst() throws Exception {
        return cursor.first();
    }

    @Override
    public void beforeFirst() throws Exception {
        throw new IllegalArgumentException("null object");
        // cursor.beforeFirst();
    }

    protected void parentCursorBeforeFirst() throws Exception {
        cursor.beforeFirst();
    }

    @Override
    public IRowSet last() throws Exception {
        throw new IllegalArgumentException("null object");
    }

    protected IRowSet parentCursorLast() throws Exception {
        return cursor.last();
    }

    //
    // @Override
    // public KVPair get(CloneableRecord key) throws Exception {
    //
    // return cursor.get(key);
    // }

    @Override
    public IRowSet getNextDup() throws Exception {
        throw new IllegalArgumentException("null object");
    }

    protected IRowSet parentCursorGetNextDup() throws Exception {
        return cursor.getNextDup();
    }

    @Override
    public List<Exception> close(List<Exception> exceptions) {
        List<Exception> ex = parentCursorClose(exceptions);
        return ex;
    }

    protected List<Exception> parentCursorClose(List<Exception> exceptions) {
        if (cursor != null) {
            List<Exception> ex = cursor.close(exceptions);
            cursor = null;
            return ex;
        }

        if (exceptions == null) exceptions = new ArrayList<Exception>();
        return exceptions;

    }

    @Override
    public boolean delete() throws Exception {
        throw new IllegalArgumentException("null object");
    }

    protected boolean parentCursorDelete() throws Exception {
        return cursor.delete();
    }

    @Override
    public void put(CloneableRecord key, CloneableRecord value) throws Exception {
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
                                                                   boolean keyFilterOrValueFilter) throws Exception {
        throw new IllegalArgumentException("null object");
    }

    protected Map<CloneableRecord, DuplicateKVPair> parentCursorMgetWithDuplicate(List<CloneableRecord> keys,
                                                                                  boolean prefixMatch,
                                                                                  boolean keyFilterOrValueFilter)
                                                                                                                 throws Exception {
        return cursor.mgetWithDuplicate(keys, prefixMatch, keyFilterOrValueFilter);
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws Exception {
        throw new IllegalArgumentException("null object");
    }

    protected List<DuplicateKVPair> parentCursorMgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                                      boolean keyFilterOrValueFilter) throws Exception {
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

    public List<ColumnMeta> parentCursorGetReturnColumns() throws Exception {
        return this.cursor.getReturnColumns();
    }
}
