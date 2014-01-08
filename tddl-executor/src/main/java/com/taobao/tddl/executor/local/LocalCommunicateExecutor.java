package com.taobao.tddl.executor.local;

import java.util.concurrent.Future;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.executor.CommunicateExecutor;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public class LocalCommunicateExecutor extends AbstractLifecycle implements CommunicateExecutor {

    @Override
    public ISchematicCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                       throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultCursor commit(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultCursor rollback(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<ISchematicCursor> execByExecPlanNodeFuture(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                                     throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<ResultCursor> commitFuture(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<ResultCursor> rollbackFuture(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

}
