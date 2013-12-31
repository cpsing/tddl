package com.taobao.tddl.matrix.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.sql.DataSource;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.MatrixExecutor;
import com.taobao.tddl.matrix.config.ConfigHolder;

/**
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:14
 * @since 5.1.0
 */
public class TDataSource extends AbstractLifecycle implements DataSource {

    private String                  ruleFilePath         = null;
    private String                  machineTopologyFile  = null;
    private String                  schemaFile           = null;
    private MatrixExecutor          executor             = null;
    private String                  appName              = null;
    private Map<String, Comparable> connectionProperties = new HashMap(2);

    private ConfigHolder            configHolder;

    /**
     * 用于并行查询的线程池
     */
    private ExecutorService         executorService      = null;

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new UnsupportedOperationException("getLogWriter");
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new UnsupportedOperationException("getLoginTimeout");
    }

    @Override
    public void setLogWriter(PrintWriter arg0) throws SQLException {
        throw new UnsupportedOperationException("setLogWriter");

    }

    @Override
    public void setLoginTimeout(int arg0) throws SQLException {
        throw new UnsupportedOperationException("setLoginTimeout");

    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.getClass().isAssignableFrom(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            return new TConnection(this);
        } catch (Exception e) {
            throw new SQLException(e);

        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return this.getConnection();
    }

    @Override
    public void doInit() throws TddlException {

        this.executor = new MatrixExecutor();

        ConfigHolder configHolder = new ConfigHolder();
        configHolder.setAppName(appName);
        configHolder.setTopologyFilePath(this.machineTopologyFile);
        configHolder.setSchemaFilePath(this.schemaFile);
        configHolder.setRuleFilePath(this.ruleFilePath);

        configHolder.init();

        this.configHolder = configHolder;

        /**
         * 如果不为每个连接都初始化，则为整个ds初始化一个线程池
         */
        if ("False".equalsIgnoreCase(GeneralUtil.getExtraCmd(this.getConnectionProperties(),
            ExtraCmd.ConnectionExtraCmd.INIT_CONCURRENT_POOL_EVERY_CONNECTION))) {
            int poolSize;
            Object poolSizeObj = GeneralUtil.getExtraCmd(this.getConnectionProperties(),
                ExtraCmd.ConnectionExtraCmd.CONCURRENT_THREAD_SIZE);

            if (poolSizeObj == null) throw new TddlRuntimeException("如果线程池为整个datasource共用，请使用CONCURRENT_THREAD_SIZE指定线程池大小");

            poolSize = Integer.valueOf(poolSizeObj.toString());

            executorService = Executors.newFixedThreadPool(poolSize, new ThreadFactory() {

                @Override
                public Thread newThread(Runnable arg0) {
                    return new Thread(arg0, "concurrent_query_executor");
                }
            });
        }

    }

    public ConfigHolder getConfigHolder() {
        return this.configHolder;
    }

    @Override
    public void doDestory() {
        // TODO Auto-generated method stub

    }

    public String getRuleFile() {
        return ruleFilePath;
    }

    public void setRuleFile(String ruleFilePath) {
        this.ruleFilePath = ruleFilePath;
    }

    public String getMachineTopologyFile() {
        return machineTopologyFile;
    }

    public void setTopologyFile(String machineTopologyFile) {
        this.machineTopologyFile = machineTopologyFile;
    }

    public String getSchemaFile() {
        return schemaFile;
    }

    public void setSchemaFile(String schemaFile) {
        this.schemaFile = schemaFile;
    }

    public MatrixExecutor getExecutor() {
        return this.executor;
    }

    public Map<String, Comparable> getConnectionProperties() {
        return this.connectionProperties;
    }

    public void setConnectionProperties(Map<String, Comparable> cp) {
        this.connectionProperties = cp;
    }

    public void setAppName(String appName) {
        this.appName = appName;

    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

}
