package com.taobao.tddl.executor;

import java.util.concurrent.Future;

import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.exception.DataAccessException;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

/**
 * 异步执行器
 * 
 * @author mengshi.sunmengshi 2013-11-28 上午11:27:36
 * @since 5.1.0
 */
@SuppressWarnings("rawtypes")
public interface IAsyncExecutor {

    /**
     * 执行一个命令
     * 
     * @param extraCmd
     * @param qc
     * @param args
     * @return
     */
    public Future<ResultCursor> execByExecPlanNodeFuture(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                                 throws DataAccessException;

}
