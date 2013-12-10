package com.taobao.tddl.repo.mysql.spi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import com.taobao.tddl.common.exception.TddlException;
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
import com.taobao.tddl.executor.spi.ITempTable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.repo.mysql.executor.TddlGroupExecutor;
import com.taobao.tddl.repo.mysql.handler.CommandExecutorFactoryMyImp;

public class My_Repository implements IRepository {

    Map<String, ITable>              tables   = new ConcurrentHashMap<String, ITable>();
    RepositoryConfig                 config;
    private CursorFactoryMyImpl      cfm;
    protected ICommandHandlerFactory cef      = null;
    protected IDataSourceGetter      dsGetter = new DatasourceMySQLImplement();

    @Override
    public ITable getTable(TableMeta meta, String groupNode, long requestID) throws TddlException {

        ITable table = tables.get(groupNode);
        if (table == null) {
            synchronized (this) {
                table = tables.get(groupNode);
                if (table == null) {
                    try {
                        table = initTable(meta, groupNode);
                    } catch (Exception ex) {
                        throw new TddlException(ExceptionErrorCodeUtils.Read_only, ex);
                    }
                    if (!meta.isTmp()) {
                        tables.put(groupNode, table);
                    }
                }

            }
        }
        if (table != null) {
            ((My_Table) table).setSelect(false);
        } else {
            throw new IllegalArgumentException("can't find table by group name :" + groupNode + " . meta" + meta);
        }
        return table;
    }

    public ITable initTable(TableMeta meta, String groupNode) throws Exception {
        DataSource ds = dsGetter.getDataSource(groupNode);
        ITable table = new My_Table(ds, meta, groupNode);
        return table;
    }

    @Override
    public void close() {

    }

    @Override
    public ITransaction beginTransaction(TransactionConfig tc) throws TddlException {
        My_Transaction my = new My_Transaction();
        my.beginTransaction();
        return my;
    }

    @Override
    public Map<String, ITable> getTables() {
        return null;
    }

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
    public void init() {
        this.config = new RepositoryConfig();
        this.config.setProperty(RepositoryConfig.DEFAULT_TXN_ISOLATION, "READ_COMMITTED");
        this.config.setProperty(RepositoryConfig.IS_TRANSACTIONAL, "true");
        cfm = new CursorFactoryMyImpl();

        cef = new CommandExecutorFactoryMyImp();

    }

    public boolean isEnhanceExecutionModel(String groupKey) {
        return false;
    }

    public void renameTable(TableMeta schema, String newName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IGroupExecutor buildGroupExecutor(Group group) {
        TGroupDataSource groupDS = new TGroupDataSource(group.getName(), group.getAppName());

        groupDS.setGroup(group);

        groupDS.init();

        TddlGroupExecutor executor = new TddlGroupExecutor();
        executor.setGroup(group);
        executor.setRemotingExecutableObject(groupDS);
        return executor;
    }

    @Override
    public ITempTable createTempTable() {
        throw new UnsupportedOperationException("temp table is not supported by mysql repo");
    }
}
