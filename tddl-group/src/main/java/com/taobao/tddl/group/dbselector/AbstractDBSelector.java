package com.taobao.tddl.group.dbselector;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import com.taobao.tddl.common.jdbc.SQLPreParser;
import com.taobao.tddl.common.jdbc.sorter.ExceptionSorter;
import com.taobao.tddl.common.jdbc.sorter.MySQLExceptionSorter;
import com.taobao.tddl.common.jdbc.sorter.OracleExceptionSorter;
import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.group.config.GroupExtraConfig;
import com.taobao.tddl.group.config.GroupIndex;
import com.taobao.tddl.group.exception.NoMoreDataSourceException;
import com.taobao.tddl.group.exception.SqlForbidException;
import com.taobao.tddl.group.jdbc.DataSourceWrapper;
import com.taobao.tddl.monitor.utils.NagiosUtils;

/**
 * @author linxuan
 * @author yangzhu
 */
public abstract class AbstractDBSelector implements DBSelector {

    private static final Logger                       logger                     = LoggerFactory.getLogger(AbstractDBSelector.class);
    private static final Map<DBType, ExceptionSorter> exceptionSorters           = new HashMap<DBType, ExceptionSorter>(2);
    static {
        exceptionSorters.put(DBType.ORACLE, new OracleExceptionSorter());
        exceptionSorters.put(DBType.MYSQL, new MySQLExceptionSorter());
    }
    private DBType                                    dbType                     = DBType.MYSQL;
    protected ExceptionSorter                         exceptionSorter            = exceptionSorters.get(dbType);
    private String                                    id                         = "undefined";                                      // id值未使用

    private static final int                          default_retryBadDbInterval = 2000;                                             // milliseconds
    protected static int                              retryBadDbInterval;                                                            // milliseconds
    static {
        int interval = default_retryBadDbInterval;
        String propvalue = System.getProperty("com.taobao.tddl.DBSelector.retryBadDbInterval");
        if (propvalue != null) {
            try {
                interval = Integer.valueOf(propvalue.trim());
            } catch (Exception e) {
                logger.error("", e);
            }
        }
        retryBadDbInterval = interval;
    }

    protected boolean                                 readable                   = false;

    public void setReadable(boolean readable) {
        this.readable = readable;
    }

    protected boolean isSupportRetry = true; // 默认情况下支持重试

    public boolean isSupportRetry() {
        return isSupportRetry;
    }

    public void setSupportRetry(boolean isSupportRetry) {
        this.isSupportRetry = isSupportRetry;
    }

    public AbstractDBSelector(){
    }

    public AbstractDBSelector(String id){
        this.id = id;
    }

    protected static class DataSourceHolder {

        public final DataSourceWrapper dsw;
        public final ReentrantLock     lock           = new ReentrantLock();
        public volatile boolean        isNotAvailable = false;
        public volatile long           lastRetryTime  = 0;

        public DataSourceHolder(DataSourceWrapper dsw){
            this.dsw = dsw;
        }
    }

    /**
     * 在一个数据库上执行，有单线程试读
     * 
     * @param <T>
     * @param dsHolder
     * @param failedDataSources
     * @param tryer
     * @param times
     * @param args
     * @return
     * @throws SQLException
     */
    protected <T> T tryOnDataSourceHolder(DataSourceHolder dsHolder, Map<DataSource, SQLException> failedDataSources,
                                          DataSourceTryer<T> tryer, int times, Object... args) throws SQLException {
        List<SQLException> exceptions = new LinkedList<SQLException>();
        if (failedDataSources != null) {
            exceptions.addAll(failedDataSources.values());
        }
        if (failedDataSources != null && failedDataSources.containsKey(dsHolder.dsw)) {
            return tryer.onSQLException(exceptions, exceptionSorter, args);
        }

        try {
            if (dsHolder.isNotAvailable) {
                boolean toTry = System.currentTimeMillis() - dsHolder.lastRetryTime > retryBadDbInterval;
                if (toTry && dsHolder.lock.tryLock()) {
                    try {
                        T t = tryer.tryOnDataSource(dsHolder.dsw, args); // 同一个时间只会有一个线程继续使用这个数据源。
                        dsHolder.isNotAvailable = false; // 用一个线程重试，执行成功则标记为可用，自动恢复
                        return t;
                    } finally {
                        dsHolder.lastRetryTime = System.currentTimeMillis();
                        dsHolder.lock.unlock();
                    }
                } else {
                    exceptions.add(new NoMoreDataSourceException("dsKey:" + dsHolder.dsw.getDataSourceKey()
                                                                 + " not Available,toTry:" + toTry));
                    return tryer.onSQLException(exceptions, exceptionSorter, args);
                }
            } else {
                return tryer.tryOnDataSource(dsHolder.dsw, args); // 有一次成功直接返回
            }
        } catch (SQLException e) {
            if (exceptionSorter.isExceptionFatal(e)) {
                NagiosUtils.addNagiosLog(NagiosUtils.KEY_DB_NOT_AVAILABLE + "|" + dsHolder.dsw.getDataSourceKey(),
                    e.getMessage());
                dsHolder.isNotAvailable = true;
            }
            exceptions.add(e);
            return tryer.onSQLException(exceptions, exceptionSorter, args);
        }
    }

    /**
     * 在指定单库上执行，不过调用方为直接设定group index的，如果指定的数据库不可用，
     * 然后又指定了ThreadLocalString.RETRY_IF_SET_DS_INDEX 为true,那么走权重(如果有权重的话)
     * 
     * @param <T>
     * @param dsHolder
     * @param failedDataSources
     * @param tryer
     * @param times
     * @param args
     * @return
     * @throws SQLException
     */
    protected <T> T tryOnDataSourceHolderWithIndex(DataSourceHolder dsHolder,
                                                   Map<DataSource, SQLException> failedDataSources,
                                                   DataSourceTryer<T> tryer, int times, GroupIndex index,
                                                   Object... args) throws SQLException {
        List<SQLException> exceptions = new LinkedList<SQLException>();
        if (failedDataSources != null) {
            exceptions.addAll(failedDataSources.values());
        }

        if (failedDataSources != null && failedDataSources.containsKey(dsHolder.dsw)) {
            return tryer.onSQLException(exceptions, exceptionSorter, args);
        }

        try {
            if (dsHolder.isNotAvailable) {
                boolean toTry = System.currentTimeMillis() - dsHolder.lastRetryTime > retryBadDbInterval;

                if (toTry && dsHolder.lock.tryLock()) {
                    try {
                        T t = tryer.tryOnDataSource(dsHolder.dsw, args); // 同一个时间只会有一个线程继续使用这个数据源。
                        dsHolder.isNotAvailable = false; // 用一个线程重试，执行成功则标记为可用，自动恢复
                        return t;
                    } finally {
                        dsHolder.lastRetryTime = System.currentTimeMillis();
                        dsHolder.lock.unlock();
                    }
                } else if (index.failRetry) {
                    // FIXME:这里需要看下，如果在事务中，是否应该重试。
                    return tryExecuteInternal(failedDataSources, tryer, times, args);
                } else {
                    exceptions.add(new NoMoreDataSourceException("dsKey:" + dsHolder.dsw.getDataSourceKey()
                                                                 + " not Available,toTry:" + toTry));
                    return tryer.onSQLException(exceptions, exceptionSorter, args);
                }
            } else {
                return tryer.tryOnDataSource(dsHolder.dsw, args); // 有一次成功直接返回
            }
        } catch (SQLException e) {
            if (exceptionSorter.isExceptionFatal(e)) {
                NagiosUtils.addNagiosLog(NagiosUtils.KEY_DB_NOT_AVAILABLE + "|" + dsHolder.dsw.getDataSourceKey(),
                    e.getMessage());
                dsHolder.isNotAvailable = true;
            }
            exceptions.add(e);
            return tryer.onSQLException(exceptions, exceptionSorter, args);
        }
    }

    protected GroupExtraConfig groupExtraConfig;

    public <T> T tryExecute(Map<DataSource, SQLException> failedDataSources, DataSourceTryer<T> tryer, int times,
                            Object... args) throws SQLException {
        // dataSourceIndex放在args最后一个.以后改动要注意
        // local set dataSourceIndex was placed first
        GroupIndex dataSourceIndex = null;
        if (args != null && args.length > 0) {
            dataSourceIndex = (GroupIndex) args[args.length - 1];
        }

        if (groupExtraConfig != null) {
            Boolean defaultMain = groupExtraConfig.isDefaultMain();
            Map<String, Integer> tableDsIndexMap = groupExtraConfig.getTableDsIndexMap();
            Map<String, Integer> sqlDsIndexMap = groupExtraConfig.getSqlDsIndexMap();
            Set<String> sqlForbidSet = groupExtraConfig.getSqlForbidSet();

            // 1.when batch ,args have no sql parameter,so,should check
            // the args
            // 2.table dataSourceIndex relation have 2th priority
            // 3.sql dataSourceIndex relation have 3th priority
            if (args != null && args.length > 0 && args[0] instanceof String) {
                if (sqlForbidSet != null && sqlForbidSet.size() > 0) {
                    String sql = (String) args[0];
                    String nomalSql = TStringUtil.fillTabWithSpace(sql);
                    boolean isForbidden = false;
                    if (sqlForbidSet.contains(nomalSql)) {
                        isForbidden = true;
                    }
                    if (!isForbidden) {
                        String actualTable = SQLPreParser.findTableName(nomalSql);
                        for (String configSql : sqlForbidSet) {
                            String nomalConfigSql = TStringUtil.fillTabWithSpace(configSql);
                            String actualConfigTable = SQLPreParser.findTableName(nomalConfigSql);
                            if (TStringUtil.isTableFatherAndSon(actualConfigTable, actualTable)) {
                                nomalConfigSql = nomalConfigSql.replaceAll(actualConfigTable, actualTable);
                            }
                            if (nomalConfigSql.equals(nomalSql)) {
                                isForbidden = true;
                                break;
                            }
                        }
                    }
                    if (isForbidden) {
                        String message = "sql : '" + sql + "' is in forbidden set.";
                        logger.error(message);
                        throw new SqlForbidException(message);
                    }
                }

                if (tableDsIndexMap != null && tableDsIndexMap.size() > 0
                    && (dataSourceIndex == null || dataSourceIndex.index == NOT_EXIST_USER_SPECIFIED_INDEX)) {
                    String sql = (String) args[0];
                    String actualTable = SQLPreParser.findTableName(sql);
                    Integer index = tableDsIndexMap.get(actualTable);
                    if (index == null || index == NOT_EXIST_USER_SPECIFIED_INDEX) {
                        Set<String> tableSet = tableDsIndexMap.keySet();
                        for (String configTable : tableSet) {
                            if (TStringUtil.isTableFatherAndSon(configTable, actualTable)) {
                                index = tableDsIndexMap.get(configTable);
                                break;
                            }
                        }
                    }
                    // 这里换了引用，外部引用是不会变的，但是最终清理的时候是同一个线程
                    // 上的threadlocal变量，所以应该不会有影响。
                    if (index != null) dataSourceIndex = new GroupIndex(index, false);
                }

                if (sqlDsIndexMap != null && sqlDsIndexMap.size() > 0
                    && (dataSourceIndex == null || dataSourceIndex.index == NOT_EXIST_USER_SPECIFIED_INDEX)) {
                    String sql = ((String) args[0]).toLowerCase();
                    String nomalSql = TStringUtil.fillTabWithSpace(sql);
                    Integer index = sqlDsIndexMap.get(nomalSql);
                    if (index == null || index == NOT_EXIST_USER_SPECIFIED_INDEX) {
                        String actualTable = SQLPreParser.findTableName(nomalSql);
                        Set<String> sqlSet = sqlDsIndexMap.keySet();
                        for (String configSql : sqlSet) {
                            String nomalConfigSql = TStringUtil.fillTabWithSpace(configSql);
                            String actualConfigTable = SQLPreParser.findTableName(nomalConfigSql);
                            if (TStringUtil.isTableFatherAndSon(actualConfigTable, actualTable)) {
                                nomalConfigSql = nomalConfigSql.replaceAll(actualConfigTable, actualTable);
                            }
                            if (nomalConfigSql.equals(nomalSql)) {
                                index = sqlDsIndexMap.get(configSql);
                                break;
                            }
                        }
                    }

                    if (index != null) dataSourceIndex = new GroupIndex(index, false);
                }
            }

            // 1.this case simple handled,just set dataSourceIndex=0
            // 2.default main have 4th priority
            if ((dataSourceIndex == null || dataSourceIndex.index == NOT_EXIST_USER_SPECIFIED_INDEX) && defaultMain) {
                dataSourceIndex = new GroupIndex(0, false);
            }
        }

        // 如果业务层直接指定了一个数据源，就直接在指定的数据源上进行查询更新操作，失败时不再重试。
        if (dataSourceIndex != null && dataSourceIndex.index != NOT_EXIST_USER_SPECIFIED_INDEX) {
            DataSourceHolder dsHolder = findDataSourceWrapperByIndex(dataSourceIndex.index);
            if (dsHolder == null) {
                throw new IllegalArgumentException("找不到索引编号为 '" + dataSourceIndex + "'的数据源");
            }
            // return tryOnDataSourceHolder(dsHolder, failedDataSources, tryer,
            // times, args);
            return tryOnDataSourceHolderWithIndex(dsHolder, failedDataSources, tryer, times, dataSourceIndex, args);
        } else {
            return tryExecuteInternal(failedDataSources, tryer, times, args);
        }
    }

    public <T> T tryExecute(DataSourceTryer<T> tryer, int times, Object... args) throws SQLException {
        return this.tryExecute(new LinkedHashMap<DataSource, SQLException>(0), tryer, times, args);
    }

    public DBType getDbType() {
        return dbType;
    }

    public void setDbType(DBType dbType) {
        this.dbType = dbType;
        this.exceptionSorter = exceptionSorters.get(this.dbType);
    }

    public final void setExceptionSorter(ExceptionSorter exceptionSorter) {
        // add by shenxun:主要还是方便测试。。构造整个dbSelector结构太复杂
        this.exceptionSorter = exceptionSorter;
    }

    public String getId() {
        return id;
    }

    // public abstract DataSource findDataSourceByIndex(int dataSourceIndex);

    protected abstract DataSourceHolder findDataSourceWrapperByIndex(int dataSourceIndex);

    protected <T> T tryExecuteInternal(DataSourceTryer<T> tryer, int times, Object... args) throws SQLException {
        return this.tryExecuteInternal(new LinkedHashMap<DataSource, SQLException>(0), tryer, times, args);
    }

    protected abstract <T> T tryExecuteInternal(Map<DataSource, SQLException> failedDataSources,
                                                DataSourceTryer<T> tryer, int times, Object... args)
                                                                                                    throws SQLException;
}
