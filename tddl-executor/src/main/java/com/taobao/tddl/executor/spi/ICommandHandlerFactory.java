package com.taobao.tddl.executor.spi;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

/**
 * 用来进行具体的，某一个Executor的，构造方法. 如果需要从执行节点层复写存储执行的过程，可以对这个接口进行修改。 比如mysql
 * 需要将原来的更新逻辑做合并。ex update set a = 1 where id = 1; 在传递中，使用的方式是一个update
 * Node,带一个QueryNode 这种查询模式，无法简单的直接被转换为一个update sql,而只能转变为一个update id in
 * .和一个select* from tab where id =1 这样的查询效率太低。
 * 因此，可以直接从updateNode这里，就将updateNode和selectNode在这个地方直接变成sql去执行。而不走到cursor层合并
 * 能够简化查询编码。
 * 
 * @author whisper
 */
public interface ICommandHandlerFactory {

    ICommandHandler getCommandHandler(IDataNodeExecutor executor, ExecutionContext executionContext);
}
