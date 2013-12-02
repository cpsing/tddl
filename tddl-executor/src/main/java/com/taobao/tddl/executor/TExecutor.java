package com.taobao.tddl.executor;

import java.util.concurrent.Future;

import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.exception.DataAccessException;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public class TExecutor implements ITransactionExecutor, ITransactionAsyncExecutor, Lifecycle {

    @Override
    public ResultCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<ResultCursor> execByExecPlanNodeFuture(IDataNodeExecutor qc,

    ExecutionContext executionContext) throws DataAccessException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void init() {
        // TODO Auto-generated method stub

    }

    @Override
    public void destory() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isInited() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Future<ResultCursor> commitFuture() throws DataAccessException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<ResultCursor> rollbackFuture() throws DataAccessException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void commit(ExecutionContext executionContext) throws DataAccessException {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollback(ExecutionContext executionContext) throws DataAccessException {
        // TODO Auto-generated method stub

    }

}
