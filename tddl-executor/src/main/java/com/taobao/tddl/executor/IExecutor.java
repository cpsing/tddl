package com.taobao.tddl.executor;

import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

/**
 * 执行器 crud 与jdbc接口类似，原则上不要进行多线程操作。 而应该使用外部方式显示的
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午3:02:29
 * @since 5.1.0
 */
@SuppressWarnings("rawtypes")
public interface IExecutor {

    /**
     * 执行一个命令
     * 
     * @param extraCmd
     * @param qc
     * @param args
     * @return
     * @throws Exception
     */
    public ResultCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext) throws Exception;
}
