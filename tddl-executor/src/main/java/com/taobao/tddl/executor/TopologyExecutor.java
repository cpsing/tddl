package com.taobao.tddl.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.spi.ITopologyExecutor;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public class TopologyExecutor extends AbstractLifecycle implements ITopologyExecutor {

    public String dataNode = "localhost";

    @Override
    public Future<ISchematicCursor> execByExecPlanNodeFuture(final IDataNodeExecutor qc,
                                                             final ExecutionContext executionContext)
                                                                                                     throws TddlException {

        final ExecutorContext executorContext = ExecutorContext.getContext();
        final OptimizerContext optimizerContext = OptimizerContext.getContext();
        ExecutorService concurrentExecutors = executionContext.getExecutorService();
        if (concurrentExecutors == null) {
            throw new TddlRuntimeException("concurrentExecutors is null, cannot query parallelly");
        }

        Future<ISchematicCursor> task = concurrentExecutors.submit(new Callable<ISchematicCursor>() {

            @Override
            public ISchematicCursor call() throws Exception {
                ExecutorContext.setContext(executorContext);
                OptimizerContext.setContext(optimizerContext);
                return execByExecPlanNode(qc, executionContext);
            }
        });
        return task;
    }

    @Override
    public ISchematicCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                       throws TddlException {
        return getGroupExecutor(qc, executionContext).execByExecPlanNode(qc, executionContext);
    }

    private IExecutor getGroupExecutor(IDataNodeExecutor qc, ExecutionContext executionContext) {
        if (executionContext == null) executionContext = new ExecutionContext();

        String group = qc.getDataNode();
        if (group == null) {
            throw new RuntimeException("group in plan is null, plan is:\n" + qc);
        }

        return getGroupExecutor(group, executionContext);
    }

    private IExecutor getGroupExecutor(String group, ExecutionContext executionContext) {
        IExecutor executor = null;
        executor = ExecutorContext.getContext().getTopologyHandler().get(group);
        if (executor == null) {
            throw new RuntimeException("cannot find executor for group:" + group + "\ngroups:\n"
                                       + ExecutorContext.getContext().getTopologyHandler());
        }

        if (!executionContext.isAutoCommit()) {
            // sql不在同一个节点上，是跨机事务，报错。
            if (executionContext.getTransactionGroup() != null && !executionContext.getTransactionGroup().equals(group)) {
                throw new TddlRuntimeException("transaction across group is not supported, aim group is:" + group
                                               + ", current transaction group is:"
                                               + executionContext.getTransactionGroup());
            }
        }

        return executor;
    }

    @Override
    public ResultCursor commit(ExecutionContext executionContext) throws TddlException {
        ResultCursor rc = new ResultCursor(null, executionContext);
        if (executionContext.getTransactionGroup() == null) {
            return rc;
        }

        return getGroupExecutor(executionContext.getTransactionGroup(), executionContext).commit(executionContext);
    }

    @Override
    public ResultCursor rollback(ExecutionContext executionContext) throws TddlException {

        ResultCursor rc = new ResultCursor(null, executionContext);
        if (executionContext.getTransactionGroup() == null) {
            return rc;
        }

        return getGroupExecutor(executionContext.getTransactionGroup(), executionContext).rollback(executionContext);
    }

    @Override
    public Future<ResultCursor> commitFuture(final ExecutionContext executionContext) throws TddlException {
        final ExecutorContext executorContext = ExecutorContext.getContext();
        final OptimizerContext optimizerContext = OptimizerContext.getContext();

        ExecutorService concurrentExecutors = executionContext.getExecutorService();

        if (concurrentExecutors == null) {
            throw new TddlRuntimeException("concurrentExecutors is null, cannot query parallelly");
        }

        Future<ResultCursor> task = concurrentExecutors.submit(new Callable<ResultCursor>() {

            @Override
            public ResultCursor call() throws Exception {
                ExecutorContext.setContext(executorContext);
                OptimizerContext.setContext(optimizerContext);
                return commit(executionContext);
            }
        });
        return task;
    }

    @Override
    public Future<ResultCursor> rollbackFuture(final ExecutionContext executionContext) throws TddlException {
        final ExecutorContext executorContext = ExecutorContext.getContext();
        final OptimizerContext optimizerContext = OptimizerContext.getContext();

        ExecutorService concurrentExecutors = executionContext.getExecutorService();

        if (concurrentExecutors == null) {
            throw new TddlRuntimeException("concurrentExecutors is null, cannot query parallelly");
        }

        Future<ResultCursor> task = concurrentExecutors.submit(new Callable<ResultCursor>() {

            @Override
            public ResultCursor call() throws Exception {
                ExecutorContext.setContext(executorContext);
                OptimizerContext.setContext(optimizerContext);
                return rollback(executionContext);
            }
        });
        return task;
    }

    @Override
    public String getDataNode() {
        return dataNode;
    }

    public void setDataNode(String dataNode) {
        this.dataNode = dataNode;
    }

}
