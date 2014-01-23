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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.MatrixExecutor;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.cursor.impl.ResultSetCursor;
import com.taobao.tddl.group.utils.GroupHintParser;
import com.taobao.tddl.matrix.jdbc.utils.ExceptionUtils;
import com.taobao.tddl.optimizer.OptimizerContext;

/**
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:06
 * @since 5.1.0
 */
public class TConnection implements Connection {

    private MatrixExecutor         executor             = null;
    private final TDataSource      ds;
    private ExecutionContext       executionContext     = new ExecutionContext();
    private final List<TStatement> openedStatements     = Collections.synchronizedList(new ArrayList<TStatement>(2));
    private boolean                isAutoCommit         = true;                                                      // jdbc规范，新连接为true
    private boolean                closed;
    private int                    transactionIsolation = -1;
    private final ExecutorService  executorService;

    public TConnection(TDataSource ds){
        this.ds = ds;
        this.executor = ds.getExecutor();
        this.executorService = ds.borrowExecutorService();
    }

    /**
     * 执行sql语句的逻辑
     */
    public ResultSet executeSQL(String sql, Map<Integer, ParameterContext> context, TStatement stmt,
                                Map<String, Object> extraCmd, ExecutionContext executionContext) throws SQLException {
        ExecutorContext.setContext(this.ds.getConfigHolder().getExecutorContext());
        OptimizerContext.setContext(this.ds.getConfigHolder().getOptimizerContext());
        ResultCursor resultCursor;
        ResultSet rs = null;
        extraCmd.putAll(buildExtraCommand(sql));
        // 处理下group hint
        String groupHint = GroupHintParser.extractTDDLGroupHint(sql);
        if (!StringUtils.isEmpty(groupHint)) {
            sql = GroupHintParser.removeTddlGroupHint(sql);
            executionContext.setGroupHint(GroupHintParser.buildTddlGroupHint(groupHint));
        }
        executionContext.setExecutorService(executorService);
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

    @Override
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

    @Override
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

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (this.isAutoCommit == autoCommit) {
            // 先排除两种最常见的状态,true==true 和false == false: 什么也不做
            return;
        }
        this.isAutoCommit = autoCommit;
        this.executionContext.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return isAutoCommit;
    }

    @Override
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

    }

    @Override
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
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new TDatabaseMetaData();
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("No operations allowed after connection closed.");
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
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

        if (executorService != null) {
            this.ds.releaseExecutorService(executorService);
        }

        if (this.executionContext != null) {
            if (this.executionContext.getTransaction() != null) {
                try {
                    this.executionContext.getTransaction().close();
                } catch (TddlException e) {
                    exceptions.add(new SQLException(e));
                }
            }

        }

        closed = true;
        ExceptionUtils.throwSQLException(exceptions, "close tconnection", Collections.EMPTY_LIST);
    }

    private Map<String, Object> buildExtraCommand(String sql) {
        Map<String, Object> extraCmd = new HashMap();
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
     * 未实现方法
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        TStatement stmt = (TStatement) createStatement();
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);
        return stmt;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                                                                                                           throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                      throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }

    /**
     * 暂时实现为isClosed
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        return this.isClosed();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        /*
         * 如果你看到这里，那么恭喜，哈哈 mysql默认在5.x的jdbc driver里面也没有实现holdability 。
         * 所以默认都是.CLOSE_CURSORS_AT_COMMIT 为了简化起见，我们也就只实现close这种
         */
        throw new UnsupportedOperationException("setHoldability");
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
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

    public boolean removeStatement(Object arg0) {
        return openedStatements.remove(arg0);
    }

    public ExecutionContext getExecutionContext() {
        return this.executionContext;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // do nothing
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        // do nothing
    }

    /**
     * 保持可读可写
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    /*---------------------后面是未实现的方法------------------------------*/

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("setSavepoint");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("setSavepoint");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("rollback");

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("releaseSavepoint");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new RuntimeException("not support exception");
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new RuntimeException("not support exception");
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("getTypeMap");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("setTypeMap");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException("nativeSQL");
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException("setCatalog");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException("getCatalog");
    }

}
