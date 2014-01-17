package com.taobao.tddl.executor.cursor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.impl.CursorMetaImp;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.ArrayRowSet;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.datatype.DataType;

public class MockArrayCursor extends AbstractLifecycle implements Cursor {

    List<IRowSet>        rows    = new ArrayList();
    Iterator<IRowSet>    iter    = null;
    private ICursorMeta  meta;
    private final String tableName;
    List<ColumnMeta>     columns = new ArrayList();
    private IRowSet      current;
    private boolean      closed  = false;

    public MockArrayCursor(String tableName){
        this.tableName = tableName;
    }

    public void addColumn(String columnName, DataType type) {
        ColumnMeta c = new ColumnMeta(this.tableName, columnName, type, null, true);
        columns.add(c);

    }

    public void addRow(Object[] values) {
        ArrayRowSet row = new ArrayRowSet(this.meta, values);
        rows.add(row);
    }

    @Override
    public void doInit() {

        iter = rows.iterator();
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public IRowSet current() throws TddlException {
        return this.current;
    }

    @Override
    public IRowSet next() throws TddlException {
        if (iter.hasNext()) {
            current = iter.next();
            return current;
        }
        current = null;
        return null;
    }

    @Override
    public IRowSet prev() throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IRowSet first() throws TddlException {
        if (rows.isEmpty()) return null;
        return rows.get(0);
    }

    @Override
    public void beforeFirst() throws TddlException {
        iter = rows.iterator();
        current = null;

    }

    @Override
    public IRowSet last() throws TddlException {
        if (rows.isEmpty()) return null;

        return rows.get(rows.size() - 1);
    }

    @Override
    public List<TddlException> close(List<TddlException> exceptions) {
        this.closed = true;
        if (exceptions == null) exceptions = new ArrayList();
        return exceptions;
    }

    @Override
    public boolean delete() throws TddlException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public IRowSet getNextDup() throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void put(CloneableRecord key, CloneableRecord value) throws TddlException {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isDone() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String toStringWithInden(int inden) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<ColumnMeta> getReturnColumns() throws TddlException {
        return columns;
    }

    public void initMeta() {
        this.meta = CursorMetaImp.buildNew(columns);

    }

    public boolean isClosed() {
        return this.closed;
    }

}
