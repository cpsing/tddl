package com.taobao.tddl.executor;

import java.util.concurrent.Future;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

/**
 * 执行器 crud 与jdbc接口类似，原则上不要进行多线程操作。 而应该使用外部方式显示的 同步异步的各一套
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午3:02:29
 * @since 5.0.0
 */
@SuppressWarnings("rawtypes")
public interface IExecutor extends Lifecycle {

    /**
     * 执行一个命令
     * 
     * @param extraCmd
     * @param qc
     * @param args
     * @return
     * @throws Exception
     */
    public ISchematicCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                       throws TddlException;

    public ResultCursor commit(ExecutionContext executionContext) throws TddlException;

    public ResultCursor rollback(ExecutionContext executionContext) throws TddlException;

    /**
     * 执行一个命令
     * 
     * @param extraCmd
     * @param qc
     * @param args
     * @return
     */
    public Future<ISchematicCursor> execByExecPlanNodeFuture(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                                     throws TddlException;

    public Future<ResultCursor> commitFuture(ExecutionContext executionContext) throws TddlException;

    public Future<ResultCursor> rollbackFuture(ExecutionContext executionContext) throws TddlException;
}
