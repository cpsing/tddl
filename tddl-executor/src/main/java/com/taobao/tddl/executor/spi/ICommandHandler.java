package com.taobao.tddl.executor.spi;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

/**
 * 命令执行处理器 比如增删改查 都属于一个命令，可能会被循环调用
 * 
 * @author whisper
 */
public interface ICommandHandler {

    /**
     * 处理对应的一个命令 具体请看实现
     * 
     * @param context
     * @param executor
     * @param executionContext
     * @return
     * @throws Exception
     */
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException;
}
