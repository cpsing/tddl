package com.taobao.tddl.matrix.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.TExecutor;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.Matrix;
import com.taobao.tddl.optimizer.config.MockRepoIndexManager;
import com.taobao.tddl.optimizer.config.table.LocalSchemaManager;
import com.taobao.tddl.optimizer.config.table.RepoSchemaManager;
import com.taobao.tddl.optimizer.config.table.parse.MatrixParser;
import com.taobao.tddl.optimizer.costbased.CostBasedOptimizer;
import com.taobao.tddl.optimizer.parse.SqlParseManager;
import com.taobao.tddl.optimizer.parse.cobar.CobarSqlParseManager;
import com.taobao.tddl.optimizer.rule.OptimizerRule;
import com.taobao.tddl.rule.TddlRule;

/**
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:14
 * @since 5.1.0
 */
public class TDataSource implements DataSource, Lifecycle {

    private final static Logger     log                  = LoggerFactory.getLogger(TDataSource.class);

    private boolean                 inited               = false;
    private String                  ruleFilePath         = null;
    private String                  machineTopologyFile  = null;
    private String                  schemaFile           = null;
    private TExecutor               executor             = null;
    private String                  appName              = null;
    private Map<String, Comparable> connectionProperties = new HashMap(2);

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
    public void init() throws TddlException {
        if (isInited()) return;

        inited = true;

        this.executor = new TExecutor();

        SqlParseManager parser = new CobarSqlParseManager();
        CostBasedOptimizer optimizer = new CostBasedOptimizer();
        parser.init();

        OptimizerContext context = new OptimizerContext();
        TddlRule tddlRule = new TddlRule();
        tddlRule.setAppRuleFile("classpath:" + this.ruleFilePath);
        tddlRule.setAppName(appName);
        tddlRule.init();

        OptimizerRule rule = new OptimizerRule(tddlRule);

        LocalSchemaManager localSchemaManager = LocalSchemaManager.parseSchema(Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream(schemaFile));

        Matrix matrix = MatrixParser.parse(Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream(machineTopologyFile));

        RepoSchemaManager schemaManager = new RepoSchemaManager();
        schemaManager.setLocal(localSchemaManager);
        schemaManager.setUseCache(true);
        schemaManager.setGroup(matrix.getGroup("andor_group_0"));
        schemaManager.init();

        context.setMatrix(matrix);
        context.setRule(rule);
        context.setSchemaManager(schemaManager);
        context.setIndexManager(new MockRepoIndexManager(schemaManager));

        OptimizerContext.setContext(context);

        optimizer.setSqlParseManager(parser);
        optimizer.init();

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

    public TExecutor getExecutor() {
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
}
