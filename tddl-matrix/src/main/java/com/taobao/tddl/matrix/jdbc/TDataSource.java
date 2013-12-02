package com.taobao.tddl.matrix.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.executor.TExecutor;

/**
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:14
 * @since 5.1.0
 */
public class TDataSource implements DataSource, Lifecycle {

    private boolean   inited = false;
    private String    ruleFilePath;
    private String    machineTopologyFile;
    private String    schemaFile;
    private TExecutor executor;

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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void init() {
        if (isInited()) return;

        inited = true;

        this.executor = new TExecutor();

        AndOrClient client = null;
        client = new AndOrClient();
        UserConfig config = new UserConfig();
        config.setUseTddlRule(this.useTddlRule);
        config.setAppName(getAppName());

        config.setAppRuleFile(getRuleFile());

        config.setMachineTopologyFile(getMachineTopologyFile());

        config.setSchemaFile(getSchemaFile());
        config.setConnectionProperties(this.getConnectionProperties());

        client.setConfigContext(config);
        client.init();

        setClient(client);

    }

    @Override
    public void destory() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isInited() {
        // TODO Auto-generated method stub
        return inited;
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

}
