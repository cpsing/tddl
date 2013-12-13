package com.taobao.tddl.repo.mysql.executor;

import java.util.concurrent.Future;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.TransactionConfig;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.exception.DataAccessException;
import com.taobao.tddl.executor.spi.ICommandHandler;
import com.taobao.tddl.executor.spi.ICommandHandlerFactory;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

/**
 * 为TGroupDatasource实现的groupexecutor
 * 因为TGroupDatasource中已经做了主备切换等功能，所以TddlGroupExecutor只是简单的执行sql
 * 
 * @author mengshi.sunmengshi 2013-12-6 下午2:39:18
 * @since 5.1.0
 */
@SuppressWarnings("rawtypes")
public class TddlGroupExecutor implements IGroupExecutor {

    public static final String TRANSACTION_GROUP_KEY = "GROUP_KEY";
    private IRepository        repo;

    /**
     * 可能是个datasource ，也可能是个rpc客户端。放在一起的原因是
     */
    private Object             remotingExecutableObject;

    private Group              group;

    @Override
    public void commit(ExecutionContext executionContext) throws TddlException {

    }

    @Override
    public Future<ISchematicCursor> commitFuture(ExecutionContext executionContext) throws TddlException {
        return null;
    }

    @Override
    public ISchematicCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                       throws TddlException {

        executionContext.setCurrentRepository(repo);
        getTransaction(executionContext, qc);

        ISchematicCursor returnCursor = null;

        returnCursor = executeInner(qc, executionContext);

        return returnCursor;
    }

    @Override
    public Future<ISchematicCursor> execByExecPlanNodeFuture(IDataNodeExecutor qc,

    ExecutionContext executionContext) throws DataAccessException {
        return null;
    }

    public ISchematicCursor executeInner(IDataNodeExecutor executor, ExecutionContext executionContext)
                                                                                                       throws TddlException {

        // 允许远程执行。在cursor里面所依赖的执行器，从本地的换为远程的。并要注意远程事务处理过程中的兼容。
        // 目前的处理方式是，走到这里的远程执行，不允许出现事务。。。出现就丢异常
        ICommandHandlerFactory commandExecutorFactory = this.repo.getCommandExecutorFactory();
        // 核心方法，用于根据当前executor,拿到对应的处理Handler
        ICommandHandler commandHandler = commandExecutorFactory.getCommandHandler(executor, executionContext);
        // command模式命令。
        return commandHandler.handle(executor, executionContext);
    }

    @Override
    public Group getGroupInfo() {
        // TODO Auto-generated method stub
        return this.group;
    }

    public Object getRemotingExecutableObject() {
        return remotingExecutableObject;
    }

    public IRepository getRepository() {
        return this.repo;
    }

    public void getTransaction(ExecutionContext executionContext, IDataNodeExecutor targetExecutor)
                                                                                                   throws TddlException {
        if (executionContext.getTransaction() != null) {
            return;
        }
        IRepository repo = executionContext.getCurrentRepository();
        boolean createTxn = executionContext.isCreateTxn();
        if (createTxn) {
            // 如果有外部的
            TransactionConfig tc = TransactionConfig.DEFAULT;

            ITransaction trans = repo.beginTransaction(tc);
            executionContext.setTransaction(trans);
            executionContext.setTransactionSequence(trans.getId());
            // 将groupNode 放入执行的上下文
            String groupNode = targetExecutor.getDataNode();
            executionContext.getExtraCmds().put(TRANSACTION_GROUP_KEY, groupNode);
        } else {
            Long transactionSequence = executionContext.getTransactionSequence();
            if (transactionSequence == null) {
                transactionSequence = TransactionConfig.AUTO_COMMIT;
            }
            executionContext.setTransactionSequence(transactionSequence);
        }
    }

    @Override
    public void rollback(ExecutionContext executionContext) throws TddlException {

    }

    @Override
    public Future<ISchematicCursor> rollbackFuture(ExecutionContext executionContext) throws TddlException {
        return null;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public void setRemotingExecutableObject(Object remotingExecutableObject) {
        this.remotingExecutableObject = remotingExecutableObject;
    }

    public void setRepository(IRepository repo) {
        this.repo = repo;
    }

    @Override
    public String toString() {
        return "GroupExecutor [groupName=" + group.getName() + ", type=" + group.getType()
               + ", remotingExecutableObject=" + remotingExecutableObject + "]";
    }
}
