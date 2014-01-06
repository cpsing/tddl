package com.taobao.tddl.matrix.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.MatrixExecutor;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.cursor.impl.ResultSetCursor;
import com.taobao.tddl.matrix.config.TDefaultConfig;
import com.taobao.tddl.matrix.jdbc.utils.ExceptionUtils;
import com.taobao.tddl.optimizer.OptimizerContext;

/**
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:06
 * @since 5.1.0
 */
public class TConnection implements Connection {

    private MatrixExecutor   executor         = null;
    private TDataSource      ds;
    private ExecutionContext executionContext = new ExecutionContext();
    private Set<TStatement>  openedStatements = new HashSet<TStatement>(2);

    public TConnection(TDataSource ds){
        this.ds = ds;
        this.executor = ds.getExecutor();
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        if (isAutoCommit) {
            executionContext = new ExecutionContext();
        } else {
            if (executionContext == null) {
                executionContext = new ExecutionContext();
                executionContext.setAutoCommit(false);
            }
        }

        TPreparedStatement stmt = new TPreparedStatement(ds, this, sql, executionContext);
        openedStatements.add(stmt);
        return stmt;
    }

    public Statement createStatement() throws SQLException {
        checkClosed();
        if (isAutoCommit) {
            executionContext = new ExecutionContext();
        } else {
            if (executionContext == null) {
                executionContext = new ExecutionContext();
                executionContext.setAutoCommit(false);
            }
        }

        TStatement stmt = new TStatement(ds, this, executionContext);
        openedStatements.add(stmt);
        return stmt;
    }

    /*
     * ========================================================================
     * JDBC事务相关的autoCommit设置、commit/rollback、TransactionIsolation等
     * ======================================================================
     */
    private boolean isAutoCommit = true; // jdbc规范，新连接为true

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (this.isAutoCommit == autoCommit) {
            // 先排除两种最常见的状态,true==true 和false == false: 什么也不做
            return;
        }
        this.isAutoCommit = autoCommit;
        this.executionContext.setAutoCommit(autoCommit);
    }

    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return isAutoCommit;
    }

    public void commit() throws SQLException {
        checkClosed();
        if (isAutoCommit) {
            return;
        }

        try {
            this.executor.commit(this.executionContext);
        } catch (TddlException e) {
            throw new SQLException(e);
        }

        clearTransactionResource();
    }

    public void rollback() throws SQLException {
        checkClosed();
        if (isAutoCommit) {
            return;
        }

        try {
            this.executor.rollback(executionContext);
        } catch (TddlException e) {
            throw new SQLException(e);
        }

        clearTransactionResource();
    }

    private void initTransactionResource() {
    }

    private void clearTransactionResource() {

    }

    // FIXME DatabaseMetaData 未实现
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new TDatabaseMetaData();
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("setSavepoint");
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("setSavepoint");
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("rollback");

    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("releaseSavepoint");

    }

    /*
     * ========================================================================
     * 关闭逻辑
     * ======================================================================
     */
    private boolean closed;

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("No operations allowed after connection closed.");
        }
    }

    public boolean isClosed() throws SQLException {
        return closed;
    }

    @SuppressWarnings("unchecked")
    public void close() throws SQLException {
        if (closed) {
            return;
        }

        List<SQLException> exceptions = new LinkedList<SQLException>();
        try {
            // 关闭statement
            for (TStatement stmt : openedStatements) {
                try {
                    stmt.close(false);
                } catch (SQLException e) {
                    exceptions.add(e);
                }
            }
        } finally {
            openedStatements.clear();
        }

        if (this.executionContext != null && this.executionContext.getTransaction() != null) {
            try {
                this.executionContext.getTransaction().setAutoCommit(true);
                this.executionContext.getTransaction().close();
            } catch (TddlException e) {
                exceptions.add(new SQLException(e));
            }
        }
        closed = true;
        ExceptionUtils.throwSQLException(exceptions, "close tconnection", Collections.EMPTY_LIST);
    }

    private Map<String, Comparable> buildExtraCommand(String sql) {
        Map<String, Comparable> extraCmd = new HashMap();
        String andorExtra = "/* ANDOR ";
        String tddlExtra = "/* TDDL ";
        if (sql != null) {
            String commet = TStringUtil.substringAfter(sql, tddlExtra);
            // 去掉注释
            if (TStringUtil.isNotEmpty(commet)) {
                commet = TStringUtil.substringBefore(commet, "*/");
            }

            if (TStringUtil.isEmpty(commet) && sql.startsWith(andorExtra)) {
                commet = TStringUtil.substringAfter(sql, andorExtra);
                commet = TStringUtil.substringBefore(commet, "*/");
            }

            if (TStringUtil.isNotEmpty(commet)) {
                String[] params = commet.split(",");
                for (String param : params) {
                    String[] keyAndVal = param.split("=");
                    if (keyAndVal.length != 2) {
                        throw new IllegalArgumentException(param + " is wrong , only key = val supported");
                    }
                    String key = keyAndVal[0];
                    String val = keyAndVal[1];
                    extraCmd.put(key, val);
                }
            }
        }
        extraCmd.putAll(this.ds.getConnectionProperties());
        return extraCmd;
    }

    /**
     * 执行sql语句的逻辑
     * 
     * @param sql
     * @param context
     * @return
     * @throws SQLException
     */
    public ResultSet executeSQL(String sql, Map<Integer, ParameterContext> context, TStatement stmt,
                                Map<String, Comparable> extraCmd, ExecutionContext executionContext)
                                                                                                    throws SQLException {
        ExecutorContext.setContext(this.ds.getConfigHolder().getExecutorContext());
        OptimizerContext.setContext(this.ds.getConfigHolder().getOptimizerContext());
        ResultCursor resultCursor;
        ResultSet rs = null;
        extraCmd.putAll(buildExtraCommand(sql));
        if (this.ds.getExecutorService() != null) {
            executionContext.setExecutorService(ds.getExecutorService());
        } else {
            Object poolSizeObj = GeneralUtil.getExtraCmd(this.ds.getConnectionProperties(),
                ExtraCmd.ConnectionExtraCmd.CONCURRENT_THREAD_SIZE);
            int poolSize = 0;
            if (poolSizeObj != null) {

                poolSize = Integer.valueOf(poolSizeObj.toString());
            } else {
                poolSize = TDefaultConfig.CONCURRENT_THREAD_SIZE;
            }

            ExecutorService executorService = Executors.newFixedThreadPool(poolSize, new ThreadFactory() {

                @Override
                public Thread newThread(Runnable arg0) {
                    return new Thread(arg0, "concurrent_query_executor");
                }
            });

            executionContext.setExecutorService(executorService);
        }
        executionContext.setParams(context);
        executionContext.setExtraCmds(extraCmd);

        try {
            resultCursor = executor.execute(sql, executionContext);
        } catch (TddlException e) {
            throw new SQLException(e);
        }

        if (resultCursor instanceof ResultSetCursor) {
            rs = ((ResultSetCursor) resultCursor).getResultSet();
        } else {
            rs = new TResultSet(resultCursor);
        }

        return rs;
    }

    /**
     * 未实现方法
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                                                                                                           throws SQLException {
        throw new UnsupportedOperationException();
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    /**
     * andor 暂时不支持GeneratedKeys
     */
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {

        throw new UnsupportedOperationException();
        // return null;
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
        // TODO Auto-generated method stub
        // return null;
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    public Clob createClob() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    public Blob createBlob() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    public NClob createNClob() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    public SQLXML createSQLXML() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    /**
     * 暂时实现为isClosed
     */
    public boolean isValid(int timeout) throws SQLException {
        // TODO 暂时实现为isClosed

        return this.isClosed();
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();

    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();

    }

    public String getClientInfo(String name) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;

    }

    public Properties getClientInfo() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public boolean isReadOnly() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return false;
    }

    public void setCatalog(String catalog) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public String getCatalog() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    public void setTransactionIsolation(int level) throws SQLException {
        // TODO Auto-generated method stub

    }

    public int getTransactionIsolation() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public void clearWarnings() throws SQLException {
        // TODO Auto-generated method stub

    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                      throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void setHoldability(int holdability) throws SQLException {
        // TODO Auto-generated method stub

    }

    public int getHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String nativeSQL(String sql) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
        // return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.getClass().isAssignableFrom(iface);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public boolean removeStatement(Object arg0) {
        return openedStatements.remove(arg0);
    }

    public ExecutionContext getExecutionContext() {
        return this.executionContext;
    }

}
