package com.taobao.tddl.matrix.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

/**
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:06
 * @since 5.1.0
 */
public class TConnection implements Connection {

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void commit() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Statement createStatement() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Statement createStatement(int arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getCatalog() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getClientInfo(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isValid(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String nativeSQL(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CallableStatement prepareCall(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollback() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollback(Savepoint arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setAutoCommit(boolean arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setCatalog(String arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setClientInfo(Properties arg0) throws SQLClientInfoException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setHoldability(int arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setReadOnly(boolean arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Savepoint setSavepoint(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setTransactionIsolation(int arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

}
