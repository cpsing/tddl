package com.taobao.tddl.executor.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.common.TransactionConfig;
import com.taobao.tddl.executor.cursor.IAffectRowCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.repo.RepositoryConfig;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITHLog;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.monitor.Monitor;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * CUD操作基类
 * 
 * @since 5.0.0
 */
public abstract class PutHandlerCommon extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(PutHandlerCommon.class);

    public PutHandlerCommon(){
        super();
    }

    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        long time = System.currentTimeMillis();
        IPut put = (IPut) executor;

        buildTableAndMeta(put, executionContext);

        int affect_rows = 0;
        ITransaction transaction = executionContext.getTransaction();
        ITable table = executionContext.getTable();
        IndexMeta meta = executionContext.getMeta();
        boolean autoCommit = false;
        try {
            if (transaction == null) {// 客户端没有用事务，这里手动加上。
                IRepository repo = executionContext.getCurrentRepository();
                if ("True".equalsIgnoreCase(repo.getRepoConfig().getProperty(RepositoryConfig.IS_TRANSACTIONAL))) {
                    transaction = repo.beginTransaction(getDefalutTransactionConfig(repo));
                    executionContext.setTransaction(transaction);
                    autoCommit = true;
                }
            }
            affect_rows = executePut(executionContext, put, table, meta);
            if (autoCommit) {
                commit(executionContext, transaction);
            }
        } catch (Exception e) {
            time = Monitor.monitorAndRenewTime(Monitor.KEY1, Monitor.ServerPut, Monitor.Key3Fail, time);
            if (autoCommit) {
                rollback(executionContext, transaction);
            }
            throw new TddlException(e);
        }

        // 这里返回key->value的方式的东西，类似Key=affectRow val=1 这样的软编码
        IAffectRowCursor affectrowCursor = executionContext.getCurrentRepository()
            .getCursorFactory()
            .affectRowCursor(executionContext, affect_rows);

        time = Monitor.monitorAndRenewTime(Monitor.KEY1, Monitor.ServerPut, Monitor.Key3Success, time);
        return affectrowCursor;

    }

    // move to JE_Transaction
    protected void rollback(ExecutionContext executionContext, ITransaction transaction) {
        try {
            // if (historyLog.get() != null) {
            // historyLog.get().rollback(transaction);
            // }
            transaction.rollback();
            executionContext.setTransaction(null);
        } catch (Exception ex) {
            logger.error("", ex);
        }
    }

    protected void commit(ExecutionContext executionContext, ITransaction transaction) throws TddlException {
        transaction.commit();
        // 清空当前事务运行状态
        executionContext.setTransaction(null);
    }

    protected abstract int executePut(ExecutionContext executionContext, IPut put, ITable table, IndexMeta meta)
                                                                                                                throws Exception;

    protected void prepare(ITransaction transaction, ITable table, IRowSet oldkv, CloneableRecord key,
                           CloneableRecord value, IPut.PUT_TYPE putType) throws TddlException {
        ITHLog historyLog = transaction.getHistoryLog();
        if (historyLog != null) {
            historyLog.parepare(transaction.getId(), table.getSchema(), putType, oldkv, new KVPair(key, value));
        }
    }

    protected TransactionConfig getDefalutTransactionConfig(IRepository repo) {
        TransactionConfig tc = new TransactionConfig();
        String isolation = repo.getRepoConfig().getProperty(RepositoryConfig.DEFAULT_TXN_ISOLATION);
        // READ_UNCOMMITTED|READ_COMMITTED|REPEATABLE_READ|SERIALIZABLE
        if ("READ_UNCOMMITTED".equals(isolation)) {
            tc.setReadUncommitted(true);
        } else if ("READ_COMMITTED".equals(isolation)) {
            tc.setReadCommitted(true);
        }
        return tc;
    }

}
