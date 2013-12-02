package com.taobao.tddl.executor;

import java.util.concurrent.Future;

import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.exception.DataAccessException;

/**
 * 带事务的异步执行器
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午3:04:25
 * @since 5.1.0
 */
public interface ITransactionAsyncExecutor extends IAsyncExecutor {

    Future<ResultCursor> commitFuture() throws DataAccessException;

    Future<ResultCursor> rollbackFuture() throws DataAccessException;
}
