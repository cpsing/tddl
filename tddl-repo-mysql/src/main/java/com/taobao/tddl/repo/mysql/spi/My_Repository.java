package com.taobao.tddl.repo.mysql.spi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.ExceptionErrorCodeUtils;
import com.taobao.tddl.executor.spi.CommandExecutorFactory;
import com.taobao.tddl.executor.spi.CursorFactory;
import com.taobao.tddl.executor.spi.DataSourceGetter;
import com.taobao.tddl.executor.spi.RemotingExecutor;
import com.taobao.tddl.executor.spi.RepositoryConfig;
import com.taobao.tddl.executor.spi.Repository;
import com.taobao.tddl.executor.spi.Table;
import com.taobao.tddl.executor.spi.TempTable;
import com.taobao.tddl.executor.spi.Transaction;
import com.taobao.tddl.executor.spi.TransactionConfig;
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import com.taobao.tddl.optimizer.config.Group;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.repo.mysql.handler.CommandExecutorFactoryMyImp;

public class My_Repository implements Repository {

    Map<String, Table>               tables   = new ConcurrentHashMap<String, Table>();
    RepositoryConfig                       config;
    private CursorFactoryMyImpl      cfm;
    protected CommandExecutorFactory cef      = null;
    protected DataSourceGetter       dsGetter = new DatasourceMySQLImplement();

    @Override
    public Table getTable(TableMeta meta, String groupNode, long requestID) throws TddlException {

        Table table = tables.get(groupNode);
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

    // @Override
    // public DataSource getDataSource(String groupNode)
    // {
    // DataSource ds = clientContext.getCurrentConfig().matrixTopology
    // .getDsGroupMap().get(groupNode);
    //
    // return ds;
    // }
    public Table initTable(TableMeta meta, String groupNode) throws Exception {
        DataSource ds = dsGetter.getDataSource(groupNode);
        Table table = new My_Table(ds, meta, groupNode);
        return table;
    }

    @Override
    public void close() {

    }

    @Override
    public Transaction beginTransaction(TransactionConfig tc) throws TddlException {
        My_Transaction my = new My_Transaction();
        my.beginTransaction();
        return my;
    }

    @Override
    public Map<String, Table> getTables() {
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
    public CursorFactory getCursorFactory() {
        return cfm;
    }

    @Override
    public CommandExecutorFactory getCommandExecutorFactory() {
        return cef;
    }

    @Override
    public void init(RepositoryConfig conf) {
        this.config = conf;

        cfm = new CursorFactoryMyImpl();

        cef = new CommandExecutorFactoryMyImp();

    }

    public boolean isEnhanceExecutionModel(String groupKey) {
        return false;
    }

    public void renameTable(TableMeta schema, String newName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    // @Override
    // public boolean removeTable(TableMeta name) throws Exception {
    // throw new UnsupportedOperationException("Not supported yet.");
    // }
    //
    // @Override
    // public int cleanLog() {
    // return 0;
    // }

    // @Override
    // public Table getTable(TableMeta meta, String groupNode,
    // boolean isTempTable, long requestID) throws Exception {
    // if (isTempTable) {
    // throw new UnsupportedOperationException("can't create table ");
    // }
    // return this.getTable(meta, groupNode, requestID);
    // }

    @Override
    public RemotingExecutor buildRemoting(Group group) {
        TGroupDataSource groupDS = new TGroupDataSource(group.getName(), group.getAppName());
        groupDS.init();
        RemotingExecutor executor = new RemotingExecutor();
        executor.setGroupName(group.getName());
        executor.setRemotingExecutableObject(groupDS);
        executor.setType(Group.GroupType.MYSQL_JDBC);
        return executor;
    }

    @Override
    public TempTable createTempTable() {
        throw new UnsupportedOperationException("temp table is not supported by mysql repo");
    }
}
