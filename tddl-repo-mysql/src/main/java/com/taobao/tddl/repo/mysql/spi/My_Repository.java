package com.taobao.tddl.repo.mysql.spi;

import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.repo.mysql.executor.TddlGroupExecutor;
import com.taobao.tddl.repo.mysql.handler.CommandHandlerFactoryMyImp;

public class My_Repository extends AbstractLifecycle implements IRepository {

    protected LoadingCache<String, LoadingCache<TableMeta, ITable>> tables;
    protected LoadingCache<Group, IGroupExecutor>                   executors;
    protected RepositoryConfig                                      config;
    protected CursorFactoryMyImpl                                   cfm;
    protected ICommandHandlerFactory                                cef      = null;
    protected IDataSourceGetter                                     dsGetter = new DatasourceMySQLImplement();
    protected PlanVisitor                                           planVisitor;

    @Override
    public void doInit() {
        this.config = new RepositoryConfig();
        this.config.setProperty(RepositoryConfig.DEFAULT_TXN_ISOLATION, "READ_COMMITTED");
        this.config.setProperty(RepositoryConfig.IS_TRANSACTIONAL, "true");
        cfm = new CursorFactoryMyImpl();
        cef = new CommandHandlerFactoryMyImp();
        tables = CacheBuilder.newBuilder().build(new CacheLoader<String, LoadingCache<TableMeta, ITable>>() {

            @Override
            public LoadingCache<TableMeta, ITable> load(final String groupNode) throws Exception {
                return CacheBuilder.newBuilder().build(new CacheLoader<TableMeta, ITable>() {

                    @Override
                    public ITable load(TableMeta meta) throws Exception {
                        try {
                            DataSource ds = dsGetter.getDataSource(groupNode);
                            My_Table table = new My_Table(ds, meta, groupNode);
                            table.setSelect(false);
                            return table;
                        } catch (Exception ex) {
                            throw new TddlException(ExceptionErrorCodeUtils.Read_only, ex);
                        }
                    }

                });
            }
        });

        executors = CacheBuilder.newBuilder().build(new CacheLoader<Group, IGroupExecutor>() {

            @Override
            public IGroupExecutor load(Group group) throws Exception {
                TGroupDataSource groupDS = new TGroupDataSource(group.getName(), group.getAppName());
                groupDS.init();

                TddlGroupExecutor executor = new TddlGroupExecutor(getRepo());
                executor.setGroup(group);
                executor.setRemotingExecutableObject(groupDS);
                return executor;
            }
        });
    }

    protected IRepository getRepo() {
        return this;
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
                return tables.get(groupNode).get(meta);
            } catch (ExecutionException e) {
                throw new TddlRuntimeException(e);
            }
        }
    }

    @Override
    public ITable getTempTable(TableMeta meta) throws TddlException {
        throw new UnsupportedOperationException("temp table is not supported by mysql repo");
    }

    @Override
    public ITransaction beginTransaction(TransactionConfig tc) throws TddlException {
        My_Transaction my = new My_Transaction(true);
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
            return executors.get(group);
        } catch (ExecutionException e) {
            throw new TddlRuntimeException(e);
        }

    }

}
