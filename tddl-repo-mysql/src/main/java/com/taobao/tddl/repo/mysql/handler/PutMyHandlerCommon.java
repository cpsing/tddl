package com.taobao.tddl.repo.mysql.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.common.TransactionConfig;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.handler.HandlerCommon;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.repo.RepositoryConfig;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.IDataSourceGetter;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITHLog;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.monitor.Monitor;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.repo.mysql.spi.DatasourceMySQLImplement;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;
import com.taobao.tddl.repo.mysql.utils.MysqlRepoUtils;

public abstract class PutMyHandlerCommon extends HandlerCommon {

    public PutMyHandlerCommon(){
        super();
        dsGetter = new DatasourceMySQLImplement();
    }

    protected IDataSourceGetter dsGetter;

    public Logger               logger = LoggerFactory.getLogger(PutMyHandlerCommon.class);

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        long time = System.currentTimeMillis();
        IPut put = (IPut) executor;
        My_JdbcHandler jdbcHandler = MysqlRepoUtils.getJdbcHandler(dsGetter, executor, executionContext);

        // 用于测试终止任务 Thread.sleep(1000000000l);
        buildTableAndMeta(put, executionContext);
        ITransaction transaction = executionContext.getTransaction();
        ITable table = executionContext.getTable();
        IndexMeta meta = executionContext.getMeta();

        boolean autoCommit = false;
        ISchematicCursor result = null;
        try {
            result = executePut(executionContext, put, table, meta, jdbcHandler);
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
        time = Monitor.monitorAndRenewTime(Monitor.KEY1, Monitor.ServerPut, Monitor.Key3Success, time);
        return result;

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
        // if (historyLog.get() != null) {
        // historyLog.get().commit(transaction);
        // }
        if (transaction != null) transaction.commit();
        // 清空当前事务运行状态
        executionContext.setTransaction(null);
    }

    protected abstract ISchematicCursor executePut(ExecutionContext executionContext, IPut put, ITable table,
                                                   IndexMeta meta, My_JdbcHandler myJdbcHandler) throws Exception;

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
