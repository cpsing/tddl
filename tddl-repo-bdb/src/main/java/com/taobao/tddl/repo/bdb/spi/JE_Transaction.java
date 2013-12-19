package com.taobao.tddl.repo.bdb.spi;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.ExceptionErrorCodeUtils;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.spi.ITHLog;
import com.taobao.tddl.executor.spi.ITransaction;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class JE_Transaction implements ITransaction {

    final AtomicReference<ITHLog> historyLog;
    Transaction                   txn;
    TransactionConfig             config;
    List<Cursor>                  openedCursors = new LinkedList<Cursor>();

    public JE_Transaction(com.sleepycat.je.Transaction txn, com.sleepycat.je.TransactionConfig config,
                          AtomicReference<ITHLog> historyLog){
        this.txn = txn;
        this.config = config;
        this.historyLog = historyLog;
    }

    @Override
    public long getId() {
        if (txn == null) {
            throw new IllegalArgumentException("事务为空");
        }
        return txn.getId();
    }

    @Override
    public void commit() throws TddlException {
        try {
            closeCursor();
            txn.commit();
            try {
                ITHLog ithLog = historyLog.get();
                if (ithLog != null) {
                    ithLog.commit(this);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } catch (ReplicaWriteException replicaWrite) {
            try {
                rollback();
            } catch (Exception throwable) {
                throw new TddlException(ExceptionErrorCodeUtils.Read_only, throwable);
            }
            throw new TddlException(ExceptionErrorCodeUtils.Read_only, replicaWrite);
        }
    }

    @Override
    public void rollback() throws TddlException {
        if (txn != null) {
            closeCursor();
            txn.abort();
            txn = null;
            try {
                ITHLog ithLog = historyLog.get();
                if (ithLog != null) {
                    ithLog.rollback(this);
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void closeCursor() throws TddlException {
        List<TddlException> ex = new ArrayList();
        for (Cursor cursor : openedCursors) {

            ex = cursor.close(ex);

        }
        if (!ex.isEmpty()) {
            throw new TddlException(ExceptionErrorCodeUtils.UNKNOWN_EXCEPTION, GeneralUtil.mergeException(ex));
        }

    }

    public ITHLog getHistoryLog() {
        return historyLog.get();
    }

    public void addCursor(Cursor cursor) {
        openedCursors.add(cursor);
    }

    public List<Cursor> getCursors() {
        return openedCursors;
    }

    @Override
    public boolean isAutoCommit() throws TddlException {
        throw new IllegalArgumentException();
    }

    @Override
    public void close() {
        throw new IllegalArgumentException("not supported yet");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        // TODO Auto-generated method stub

    }
}
