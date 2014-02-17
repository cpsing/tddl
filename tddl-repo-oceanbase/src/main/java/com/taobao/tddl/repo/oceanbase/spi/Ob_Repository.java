package com.taobao.tddl.repo.oceanbase.spi;

import javax.sql.DataSource;

import com.alipay.oceanbase.OceanbaseDataSourceProxy;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.Group;
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
import com.taobao.tddl.repo.mysql.spi.My_Repository;
import com.taobao.tddl.repo.oceanbase.handler.ObCommandHandlerFactory;

public class Ob_Repository extends My_Repository implements IRepository {

    protected IDataSourceGetter dsGetter   = new ObDatasourceGetter();
    public static final String  CONFIG_URL = "CONFIGURL";

    @Override
    public void doInit() {
        this.config = new RepositoryConfig();
        this.config.setProperty(RepositoryConfig.DEFAULT_TXN_ISOLATION, "READ_COMMITTED");
        this.config.setProperty(RepositoryConfig.IS_TRANSACTIONAL, "true");
        cfm = new ObCursorFactory();
        cef = new ObCommandHandlerFactory();

        tables = CacheBuilder.newBuilder().build(new CacheLoader<String, LoadingCache<TableMeta, ITable>>() {

            @Override
            public LoadingCache<TableMeta, ITable> load(final String groupNode) throws Exception {
                return CacheBuilder.newBuilder().build(new CacheLoader<TableMeta, ITable>() {

                    @Override
                    public ITable load(TableMeta meta) throws Exception {
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
            }
        });

        executors = CacheBuilder.newBuilder().build(new CacheLoader<Group, IGroupExecutor>() {

            @Override
            public IGroupExecutor load(Group group) throws Exception {
                OceanbaseDataSourceProxy obDS = new OceanbaseDataSourceProxy();
                String configUrl = group.getProperties().get(CONFIG_URL);

                if (configUrl == null) {
                    throw new TddlRuntimeException("config url is not assigned, oceanbase datasource cannot be inited");
                }

                obDS.setConfigURL(configUrl);
                obDS.init();

                TddlGroupExecutor executor = new TddlGroupExecutor(getRepo());
                executor.setGroup(group);
                executor.setRemotingExecutableObject(obDS);
                return executor;
            }
        });
    }

    @Override
    public ITransaction beginTransaction(TransactionConfig tc) throws TddlException {
        Ob_Transaction my = new Ob_Transaction(true);
        my.beginTransaction();
        return my;
    }

    @Override
    public ICursorFactory getCursorFactory() {
        return cfm;
    }

    @Override
    public ICommandHandlerFactory getCommandExecutorFactory() {
        return cef;
    }

}
