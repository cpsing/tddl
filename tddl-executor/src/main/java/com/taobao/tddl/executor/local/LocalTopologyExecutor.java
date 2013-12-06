package com.taobao.tddl.executor.local;

import java.util.concurrent.Future;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.ExecutorContext;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.executor.spi.ITopologyExecutor;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

@SuppressWarnings("rawtypes")
public class LocalTopologyExecutor implements ITopologyExecutor {

    public String dataNode = "localhost";

    @Override
    public Future<ISchematicCursor> execByExecPlanNodeFuture(IDataNodeExecutor qc,

    ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ISchematicCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                       throws TddlException {

        if (executionContext == null) executionContext = new ExecutionContext();

        String group = qc.getDataNode();
        if (group == null) {
            throw new RuntimeException("group in plan is null, plan is:\n" + qc);
        }

        IGroupExecutor executor = null;

        executor = ExecutorContext.getContext().getTopologyHandler().get(group);

        if (executor == null) {
            throw new RuntimeException("cannot find executor for group:" + group + "\ngroups:\n"
                                       + ExecutorContext.getContext().getTopologyHandler());
        }

        return executor.execByExecPlanNode(qc, executionContext);

    }

    @Override
    public void commit(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollback(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub

    }

    @Override
    public Future<ISchematicCursor> commitFuture(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<ISchematicCursor> rollbackFuture(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getDataNode() {
        return dataNode;
    }

    public void setDataNode(String dataNode) {
        this.dataNode = dataNode;
    }

}
