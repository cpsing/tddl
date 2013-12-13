package com.taobao.tddl.matrix.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.MatrixExecutor;
import com.taobao.tddl.matrix.config.ConfigHolder;

/**
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:14
 * @since 5.1.0
 */
public class TDataSource extends AbstractLifecycle implements DataSource {

    private final static Logger     log                  = LoggerFactory.getLogger(TDataSource.class);

    private String                  ruleFilePath         = null;
    private String                  machineTopologyFile  = null;
    private String                  schemaFile           = null;
    private MatrixExecutor          executor             = null;
    private String                  appName              = null;
    private Map<String, Comparable> connectionProperties = new HashMap(2);

    private ConfigHolder            configHolder;

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setLogWriter(PrintWriter arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setLoginTimeout(int arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
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

        configHolder.init();

        this.configHolder = configHolder;

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

    public void setMachineTopologyFile(String machineTopologyFile) {
        this.machineTopologyFile = machineTopologyFile;
    }

    public String getSchemaFile() {
        return schemaFile;
    }

    public void setSchemaFile(String schemaFile) {
        this.schemaFile = schemaFile;
    }

    public MatrixExecutor getExecutor() {
        // TODO Auto-generated method stub
        return this.executor;
    }

    public Map<String, Comparable> getConnectionProperties() {
        // TODO Auto-generated method stub
        return this.connectionProperties;
    }

    public void setConnectionProperties(Map<String, Comparable> cp) {
        this.connectionProperties = cp;
    }

    public void setAppName(String appName) {
        this.appName = appName;

    }
}
