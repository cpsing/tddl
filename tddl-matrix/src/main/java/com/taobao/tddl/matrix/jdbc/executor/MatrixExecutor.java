package com.taobao.tddl.matrix.jdbc.executor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import com.taobao.tddl.executor.ITransactionAsyncExecutor;
import com.taobao.tddl.executor.ITransactionExecutor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.exception.DataAccessException;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

@SuppressWarnings("rawtypes")
public class MatrixExecutor implements ITransactionAsyncExecutor, ITransactionExecutor {

    Map<String, GroupExecutor> groupExecutors = new HashMap();

    @Override
    public Future<ResultCursor> execByExecPlanNodeFuture(IDataNodeExecutor qc,

    ExecutionContext executionContext) throws DataAccessException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext) throws Exception {
        ResultCursor cursor = null;
        ISchematicCursor iSchematicCursor = null;

        if (executionContext == null) executionContext = new ExecutionContext();

        String group = qc.getDataNode();

        if (group == null) {
            throw new RuntimeException("group in plan is null, plan is:\n" + qc);
        }
        GroupExecutor groupExecutor = this.groupExecutors.get(group);

        if (groupExecutor == null) {
            throw new RuntimeException("cannot find group executor for group:" + group + "\ngroups:\n"
                                       + this.groupExecutors);
        }

        return groupExecutor.execByExecPlanNode(qc, executionContext);

    }

    @Override
    public void commit(ExecutionContext executionContext) throws DataAccessException {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollback(ExecutionContext executionContext) throws DataAccessException {
        // TODO Auto-generated method stub

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

}
