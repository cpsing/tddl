package com.taobao.tddl.repo.oceanbase.spi;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import com.alipay.oceanbase.OceanbaseDataSourceProxy;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.ExceptionErrorCodeUtils;
import com.taobao.tddl.executor.common.TransactionConfig;
import com.taobao.tddl.executor.repo.RepositoryConfig;
import com.taobao.tddl.executor.spi.ICommandHandlerFactory;
import com.taobao.tddl.executor.spi.ICursorFactory;
import com.taobao.tddl.executor.spi.IDataSourceGetter;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.repo.mysql.executor.TddlGroupExecutor;
import com.taobao.tddl.repo.oceanbase.handler.ObCommandHandlerFactory;

public class Ob_Repository extends AbstractLifecycle implements IRepository {

    protected Cache<String, ITable>        tables     = CacheBuilder.newBuilder().build();
    protected Cache<Group, IGroupExecutor> executors  = CacheBuilder.newBuilder().build();
    protected RepositoryConfig             config;
    protected ObCursorFactory              cfm;
    protected ICommandHandlerFactory       cef        = null;
    protected IDataSourceGetter            dsGetter   = new ObDatasourceGetter();
    public static final String             CONFIG_URL = "CONFIGURL";

    @Override
    public void doInit() {
        this.config = new RepositoryConfig();
        this.config.setProperty(RepositoryConfig.DEFAULT_TXN_ISOLATION, "READ_COMMITTED");
        this.config.setProperty(RepositoryConfig.IS_TRANSACTIONAL, "true");
        cfm = new ObCursorFactory();
        cef = new ObCommandHandlerFactory();

    }

    @Override
    protected void doDestory() throws TddlException {
        tables.cleanUp();

        for (IGroupExecutor executor : executors.asMap().values()) {
            executor.destory();
        }
    }

    @Override
    public ITable getTable(final TableMeta meta, final String groupNode) throws TddlException {
        if (meta.isTmp()) {
            return getTempTable(meta);
        } else {
            try {
                return tables.get(groupNode, new Callable<ITable>() {

                    @Override
                    public ITable call() throws Exception {
                        try {
                            DataSource ds = dsGetter.getDataSource(groupNode);
                            Ob_Table table = new Ob_Table(ds, meta, groupNode);
                            table.setSelect(false);
                            return table;
                        } catch (Exception ex) {
                            throw new TddlException(ExceptionErrorCodeUtils.Read_only, ex);
                        }
                    }
                });
            } catch (ExecutionException e) {
                throw new TddlException(e);
            }
        }
    }

    @Override
    public ITable getTempTable(TableMeta meta) throws TddlException {
        throw new UnsupportedOperationException("temp table is not supported by mysql repo");
    }

    @Override
    public ITransaction beginTransaction(TransactionConfig tc) throws TddlException {
        Ob_Transaction my = new Ob_Transaction(true);
        my.beginTransaction();
        return my;
    }

    @Override
    public RepositoryConfig getRepoConfig() {
        return config;
    }

    @Override
    public boolean isWriteAble() {
        return true;
    }

    @Override
    public ICursorFactory getCursorFactory() {
        return cfm;
    }

    @Override
    public ICommandHandlerFactory getCommandExecutorFactory() {
        return cef;
    }

    @Override
    public boolean isEnhanceExecutionModel(String groupKey) {
        return false;
    }

    @Override
    public IGroupExecutor getGroupExecutor(final Group group) {
        try {
            final IRepository repo = (this);
            return executors.get(group, new Callable<IGroupExecutor>() {

                @Override
                public IGroupExecutor call() throws Exception {
                    OceanbaseDataSourceProxy obDS = new OceanbaseDataSourceProxy();
                    String configUrl = group.getProperties().get(CONFIG_URL);

                    if (configUrl == null) throw new TddlRuntimeException("config url is not assigned, oceanbase datasource cannot be inited");

                    obDS.setConfigURL(configUrl);
                    obDS.init();

                    TddlGroupExecutor executor = new TddlGroupExecutor(repo);
                    executor.setGroup(group);
                    executor.setRemotingExecutableObject(obDS);
                    return executor;
                }
            });
        } catch (ExecutionException e) {
            throw new TddlRuntimeException(e);
        }

    }
}
