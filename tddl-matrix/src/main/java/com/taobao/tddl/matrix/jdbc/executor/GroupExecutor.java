package com.taobao.tddl.matrix.jdbc.executor;

import java.util.concurrent.Future;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.ITransactionAsyncExecutor;
import com.taobao.tddl.executor.ITransactionExecutor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.exception.DataAccessException;
import com.taobao.tddl.executor.spi.CommandExecutorFactory;
import com.taobao.tddl.executor.spi.CommandHandler;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.executor.spi.Repository;
import com.taobao.tddl.executor.spi.Transaction;
import com.taobao.tddl.executor.spi.TransactionConfig;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;

@SuppressWarnings("rawtypes")
public class GroupExecutor implements ITransactionAsyncExecutor, ITransactionExecutor {

    public static final String TRANSACTION_GROUP_KEY = "MATRIX_KEY";
    private Repository         repo;

    @Override
    public Future<ResultCursor> execByExecPlanNodeFuture(IDataNodeExecutor qc,

    ExecutionContext executionContext) throws DataAccessException {
        return null;
    }

    @Override
    public ResultCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                   throws TddlException {
        // ResultCursor rc = executeOnOthers(context, command,
        // executionContext);
        getTransaction(executionContext, qc);
        Repository repo = executionContext.getCurrentRepository();
        ISchematicCursor returnCursor = null;

        returnCursor = executeInner(qc, executionContext);

        return wrapResultCursor(qc, returnCursor, executionContext);
    }

    public ISchematicCursor executeInner(IDataNodeExecutor executor, ExecutionContext executionContext)
                                                                                                       throws TddlException {

        // 允许远程执行。在cursor里面所依赖的执行器，从本地的换为远程的。并要注意远程事务处理过程中的兼容。
        // 目前的处理方式是，走到这里的远程执行，不允许出现事务。。。出现就丢异常
        CommandExecutorFactory commandExecutorFactory = this.repo.getCommandExecutorFactory();
        // 核心方法，用于根据当前executor,拿到对应的处理Handler
        CommandHandler commandHandler = commandExecutorFactory.getCommandHandler(executor, executionContext);
        // command模式命令。
        return commandHandler.handle(executor, executionContext);
    }

    @Override
    public void commit(ExecutionContext executionContext) throws TddlException {

    }

    @Override
    public void rollback(ExecutionContext executionContext) throws TddlException {

    }

    @Override
    public Future<ResultCursor> commitFuture(ExecutionContext executionContext) throws TddlException {
        return null;
    }

    @Override
    public Future<ResultCursor> rollbackFuture(ExecutionContext executionContext) throws TddlException {
        return null;
    }

    public void getTransaction(ExecutionContext executionContext, IDataNodeExecutor targetExecutor)
                                                                                                   throws TddlException {
        if (executionContext.getTransaction() != null) {
            return;
        }
        Repository repo = executionContext.getCurrentRepository();
        boolean createTxn = executionContext.isCreateTxn();
        if (createTxn) {
            // 如果有外部的
            TransactionConfig tc = TransactionConfig.DEFAULT;

            Transaction trans = repo.beginTransaction(tc);
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

    protected ResultCursor wrapResultCursor(IDataNodeExecutor command, ISchematicCursor iSchematicCursor,
                                            ExecutionContext context) throws TddlException {
        ResultCursor cursor;
        // 包装为可以传输的ResultCursor
        if (command instanceof IQueryTree) {
            if (!(iSchematicCursor instanceof ResultCursor)) {

                cursor = new ResultCursor(iSchematicCursor, context.getExtraCmds());
            } else {
                cursor = (ResultCursor) iSchematicCursor;
            }

        } else {
            if (!(iSchematicCursor instanceof ResultCursor)) {
                cursor = new ResultCursor(iSchematicCursor, context.getExtraCmds());
            } else {
                cursor = (ResultCursor) iSchematicCursor;
            }
        }
        generateResultIdAndPutIntoResultSetMap(cursor);
        return cursor;
    }

    private void generateResultIdAndPutIntoResultSetMap(ResultCursor cursor) {
        int id = idGen.getIntegerNextNumber();
        cursor.setResultID(id);

    }

    /**
     * id 生成器
     */
    private AtomicNumberCreator idGen = AtomicNumberCreator.getNewInstance();

}
