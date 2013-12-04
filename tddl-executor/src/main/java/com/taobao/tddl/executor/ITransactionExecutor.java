package com.taobao.tddl.executor;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.spi.ExecutionContext;

/**
 * 带事务的执行器
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午3:04:15
 * @since 5.1.0
 */
public interface ITransactionExecutor extends IExecutor {

    void commit(ExecutionContext executionContext) throws TddlException;

    void rollback(ExecutionContext executionContext) throws TddlException;
}
