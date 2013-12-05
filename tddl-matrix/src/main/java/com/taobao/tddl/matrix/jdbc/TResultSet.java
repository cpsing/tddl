package com.taobao.tddl.matrix.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ICursorMeta;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:23
 * @since 5.1.0
 */
public class TResultSet implements ResultSet {

    /** Has this result set been closed? */
    protected boolean             isClosed                  = false;

    public ResultCursor           resultCursor;

    private IRowSet               currentKVPair;
    private IRowSet               cacheRowSetToBuildMeta    = null;
    private TResultSetMetaData    resultSetMetaData         = null;
    private boolean               wasNull;
    private boolean               isLoigcalIndexEqualActualIndex;

    private Map<Integer, Integer> logicalIndexToActualIndex = null;

    public int getAffectRows() throws SQLException {
        // return resultSursor.getAffectRows();
        if (next()) {
            Integer index = getIndexByColumnLabel(ResultCursor.AFFECT_ROW);
            return currentKVPair.getInteger(index);
        } else {
            return 0;
        }
    }

    public TResultSet(ResultCursor resultCursor){
        this.resultCursor = resultCursor;
        if (this.resultCursor != null && this.resultCursor.getOriginalSelectColumns() != null
            && !this.resultCursor.getOriginalSelectColumns().isEmpty()) this.resultSetMetaData = new TResultSetMetaData(ExecUtils.convertIColumnsToColumnMeta(this.resultCursor.getOriginalSelectColumns()));

    }

    // 游标指向下一跳记录
    public boolean next() throws SQLException {

        checkClosed();
        IRowSet kvPair;
        try {
            if (cacheRowSetToBuildMeta != null) {
                kvPair = cacheRowSetToBuildMeta;
                cacheRowSetToBuildMeta = null;
            } else kvPair = resultCursor.next();

            this.currentKVPair = kvPair;
        } catch (Exception e) {
            this.currentKVPair = null;
            throw new SQLException(e);

        }
        if (null != kvPair) {
            return true;
        } else {
            return false;
        }
    }

    private void checkClosed() throws SQLException {
        if (this.isClosed) {
            throw new SQLException("ResultSet.Operation_not_allowed_after_ResultSet_closed");
        }

    }

    public void close() throws SQLException {
        if (isClosed) {
            return;
        }
        try {
            this.resultSetMetaData = null;
            List<TddlException> exs = new ArrayList();
            exs = this.resultCursor.close(exs);
            if (!exs.isEmpty()) throw GeneralUtil.mergeException(exs);
            isClosed = true;
        } catch (Exception e) {
            throw new SQLException(e);

        }
    }

    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    private void validateColumnLabel(String columnLabel) throws SQLException {
        if (!this.getMetaData().columnIsExist(columnLabel)) {
            throw new SQLException("column " + columnLabel + " doesn't exist!, " + this.getMetaData().getColumnMetas());
        }
    }

    private DATA_TYPE getColumnLabelDataType(String columnLabel) throws SQLException {
        return this.getMetaData().getColumnDataType(columnLabel);

    }

    private void validateColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 0 || columnIndex > this.getMetaData().getColumnCount()) {
            throw new SQLException("columnIndex 越界，column size：" + this.getMetaData().getColumnCount());
        }
    }

    /**
     * @param logicalIndex 用户select时的index
     * @return IRowSet中实际的index
     * @throws SQLException
     */
    private int getActualIndex(int logicalIndex) {
        if (this.logicalIndexToActualIndex == null) {
            logicalIndexToActualIndex = new HashMap<Integer, Integer>();
            isLoigcalIndexEqualActualIndex = true;
            ICursorMeta cm = null;

            if (this.currentKVPair == null) return logicalIndex;

            cm = currentKVPair.getParentCursorMeta();
            if (cm.isSureLogicalIndexEqualActualIndex()) {
                // 如果确定相等，就不需要挨个去判断了
                isLoigcalIndexEqualActualIndex = true;
            } else {
                try {
                    for (int i = 0; i < this.getMetaData().getColumnCount(); i++) {
                        ColumnMeta ic = this.getMetaData().getColumnMetas().get(i);
                        String name = ic.getName();
                        String tableName = ic.getTableName();
                        Integer indexInCursorMeta = cm.getIndex(tableName, name);

                        if (indexInCursorMeta == null && ic.getAlias() != null) {
                            indexInCursorMeta = cm.getIndex(tableName, ic.getAlias());
                        }

                        if (indexInCursorMeta == null) {
                            throw new RuntimeException("impossible");
                        }
                        logicalIndexToActualIndex.put(i, indexInCursorMeta);
                        if (i != indexInCursorMeta) {
                            isLoigcalIndexEqualActualIndex = false;
                        }
                    }

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (isLoigcalIndexEqualActualIndex) {
            return logicalIndex;
        } else {
            Integer actualIndex = logicalIndexToActualIndex.get(logicalIndex);

            return actualIndex;
        }
    }

    public String getString(String columnLabel) throws SQLException {
        Integer index = getIndexByColumnLabel(columnLabel);
        String str = currentKVPair.getString(index);
        if (str == null) {
            wasNull = true;
            return str;
        } else {
            wasNull = false;
            return str;
        }
    }

    protected Integer getIndexByColumnLabel(String columnLabel) throws SQLException {
        columnLabel = TStringUtil.upperCase(columnLabel);
        String table = null;
        String name = null;
        boolean contains = TStringUtil.contains(columnLabel, ".")
                           & !(TStringUtil.contains(columnLabel, "(") & TStringUtil.contains(columnLabel, ")"));
        if (contains) {
            String[] ss = TStringUtil.split(columnLabel, ".");
            if (ss.length != 2) {
                throw new SQLException("lab can only has one dot ");
            }
            table = ss[0];
            name = ss[1];
        } else {
            name = columnLabel;
        }
        if (currentKVPair == null) {
            throw new IllegalStateException("兄弟确定调用了rs.next并返回true了么？");
        }
        Integer index = currentKVPair.getParentCursorMeta().getIndex(table, columnLabel);
        // if (index == null) {
        // // throw new SQLException("can't find index by table " + table
        // // + " . column " + name);
        // }
        return index;
    }

    public String getString(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        String str = currentKVPair.getString(getActualIndex(columnIndex));
        if (str == null) {
            wasNull = true;
            return str;
        } else {
            wasNull = false;
            return str;
        }
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Boolean bool = currentKVPair.getBoolean(getActualIndex(columnIndex));
        if (null == bool) {
            wasNull = true;
            return false;
        } else {
            wasNull = false;
            return bool;
        }
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        validateColumnLabel(columnLabel);
        Integer index = getIndexByColumnLabel(columnLabel);
        Boolean bool = currentKVPair.getBoolean(index);
        if (null == bool) {
            wasNull = true;
            return false;
        } else {
            wasNull = false;
            return bool;
        }
    }

    public short getShort(String columnLabel) throws SQLException {
        validateColumnLabel(columnLabel);
        Integer index = getIndexByColumnLabel(columnLabel);
        Short st = currentKVPair.getShort(index);
        if (st == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return st;
        }
    }

    public short getShort(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Short st = currentKVPair.getShort(getActualIndex(columnIndex));
        if (st == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return st;
        }
    }

    public int getInt(String columnLabel) throws SQLException {
        validateColumnLabel(columnLabel);
        Integer index = getIndexByColumnLabel(columnLabel);
        if (index == null) {
            wasNull = true;
            return 0;
        }

        Integer inte = currentKVPair.getInteger(index);
        if (inte == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return inte;
        }
    }

    public int getInt(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Integer inte = currentKVPair.getInteger(getActualIndex(columnIndex));
        if (inte == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return inte;
        }
    }

    public long getLong(String columnLabel) throws SQLException {
        validateColumnLabel(columnLabel);
        Integer index = getIndexByColumnLabel(columnLabel);
        if (index == null) {
            wasNull = true;
            return 0l;
        }

        Long l = currentKVPair.getLong(index);
        if (l == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return l;
        }
    }

    public long getLong(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Long l = currentKVPair.getLong(getActualIndex(columnIndex));
        if (l == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return l;
        }
    }

    public float getFloat(String columnLabel) throws SQLException {
        validateColumnLabel(columnLabel);
        Integer index = getIndexByColumnLabel(columnLabel);
        if (index == null) {
            wasNull = true;
            return (float) 0.0;
        }

        Float fl = currentKVPair.getFloat(index);
        if (fl == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return fl;
        }
    }

    public float getFloat(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Float fl = currentKVPair.getFloat(getActualIndex(columnIndex));
        if (fl == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return fl;
        }
    }

    public double getDouble(String columnLabel) throws SQLException {
        validateColumnLabel(columnLabel);
        Integer index = getIndexByColumnLabel(columnLabel);
        if (index == null) {
            wasNull = true;
            return 0.0;
        }

        Double doub = currentKVPair.getDouble(index);
        if (doub == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return doub;
        }
    }

    public double getDouble(int columnIndex) throws SQLException {

        validateColumnIndex(columnIndex);
        columnIndex--;
        Double doub = currentKVPair.getDouble(getActualIndex(columnIndex));
        if (doub == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return doub;
        }
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        validateColumnLabel(columnLabel);
        Integer index = getIndexByColumnLabel(columnLabel);
        if (index == null) {
            wasNull = true;
            return null;
        }

        byte[] bytes = currentKVPair.getBytes(index);
        if (bytes == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return bytes;
        }
    }

    public byte[] getBytes(int columnIndex) throws SQLException {

        validateColumnIndex(columnIndex);
        columnIndex--;
        byte[] bytes = currentKVPair.getBytes(getActualIndex(columnIndex));
        if (bytes == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return bytes;
        }
    }

    // ustore 将date按long型对待
    public Date getDate(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Date date = currentKVPair.getDate(getActualIndex(columnIndex));
        if (date == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return date;
        }
    }

    public Date getDate(String columnLabel) throws SQLException {

        validateColumnLabel(columnLabel);
        Integer index = getIndexByColumnLabel(columnLabel);
        if (index == null) {
            wasNull = true;
            return null;
        }
        Date date = currentKVPair.getDate(index);
        if (date == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return date;
        }
    }

    public byte getByte(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        byte[] bytes = currentKVPair.getBytes(getActualIndex(columnIndex));
        if (bytes == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return bytes[0];
        }
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Timestamp ts = currentKVPair.getTimestamp(getActualIndex(columnIndex));
        if (ts == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return ts;
        }
    }

    /* 为实现方法 */
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(columnIndex);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public Time getTime(int columnIndex) throws SQLException {
        Timestamp ts = getTimestamp(columnIndex);
        if (ts == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        return new Time(ts.getTime());
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();

    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new UnsupportedOperationException();

    }

    public byte getByte(String columnLabel) throws SQLException {
        validateColumnLabel(columnLabel);
        Integer index = getIndexByColumnLabel(columnLabel);
        if (index == null) return 0;
        byte[] bytes = currentKVPair.getBytes(index);
        if (bytes == null || bytes.length == 0) {
            return 0;
        } else {
            return bytes[0];
        }
    }

    public Time getTime(String columnLabel) throws SQLException {
        Timestamp ts = getTimestamp(columnLabel);
        if (ts == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        return new Time(ts.getTime());
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        validateColumnLabel(columnLabel);

        Integer index = getIndexByColumnLabel(columnLabel);
        if (index == null) {
            wasNull = true;
            return null;
        }
        Timestamp ts = currentKVPair.getTimestamp(index);
        if (ts == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return ts;
        }
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public SQLWarning getWarnings() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void clearWarnings() throws SQLException {

        throw new UnsupportedOperationException();

    }

    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public TResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        if (this.resultSetMetaData != null) return this.resultSetMetaData;
        IRowSet kvPair = null;
        if (this.currentKVPair != null) {
            kvPair = currentKVPair;

        } else if (this.cacheRowSetToBuildMeta != null) {

        } else {
            try {
                cacheRowSetToBuildMeta = resultCursor.next();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        kvPair = cacheRowSetToBuildMeta;
        if (kvPair == null) {
            resultSetMetaData = new TResultSetMetaData(new ArrayList(0));
        } else {
            resultSetMetaData = new TResultSetMetaData(kvPair.getParentCursorMeta().getColumns());
        }

        return resultSetMetaData;
    }

    public Object getObject(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Object obj = currentKVPair.getObject(getActualIndex(columnIndex));
        if (obj == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return obj;
        }
    }

    public Object getObject(String columnLabel) throws SQLException {
        validateColumnLabel(columnLabel);
        // 对日期类型特殊处理
        DATA_TYPE a = getColumnLabelDataType(columnLabel);
        boolean dataFlag = false;
        if (a != null) {
            dataFlag = a.name().endsWith("DATE_VAL");
        } else {
            dataFlag = false;
        }
        try {

            Integer index = getIndexByColumnLabel(columnLabel);
            if (index == null)
            // throw new
            // RuntimeException("cannot find column "+columnLabel+" in result set. meta is"
            // +currentKVPair.getParentCursorMeta());
            {
                wasNull = true;
                return null;
            }

            Object result = currentKVPair.getObject(index);
            if (null == result) {
                wasNull = true;
                return null;
            } else if (dataFlag) {
                // 为了保持和mysql数据库date类型返回值一直对date类型数据 进行特殊的处理
                java.sql.Date date_value = null;
                if (result instanceof java.util.Date) {
                    date_value = new java.sql.Date(((java.util.Date) result).getTime());
                } else {
                    date_value = (java.sql.Date) result;
                }
                Calendar dateCal = Calendar.getInstance();
                dateCal.setTime(date_value);

                int year = dateCal.get(Calendar.YEAR);
                int month = dateCal.get(Calendar.MONTH);
                int day = dateCal.get(Calendar.DAY_OF_MONTH);

                dateCal.set(year, month, day, 0, 0, 0);
                dateCal.set(Calendar.MILLISECOND, 0);

                long dateAsMillis = 0;

                try {
                    dateAsMillis = dateCal.getTimeInMillis();
                } catch (IllegalAccessError iae) {
                    // Must be on JDK-1.3.1 or older....
                    dateAsMillis = dateCal.getTime().getTime();
                }
                wasNull = false;
                return new Date(dateAsMillis);
            } else {
                wasNull = false;
                return result;
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public int findColumn(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {

        validateColumnIndex(columnIndex);
        Object value = getObject(columnIndex);

        if (value == null) {
            wasNull = true;
            return null;
        }

        wasNull = false;

        return this.validBigDecimal(value);
    }

    BigDecimal validBigDecimal(Object value) {
        if (value instanceof BigDecimal) return (BigDecimal) value;

        if (value instanceof Long) return new BigDecimal((Long) value);

        if (value instanceof Short) return new BigDecimal((Short) value);

        if (value instanceof Double) return new BigDecimal((Double) value);

        if (value instanceof Float) return new BigDecimal((Float) value);

        if (value instanceof Integer) return new BigDecimal((Integer) value);

        if (value instanceof BigInteger) return new BigDecimal((BigInteger) value);

        if (value instanceof String) return new BigDecimal((String) value);

        if (value instanceof Date) return new BigDecimal(((Date) value).getTime());

        throw new RuntimeException("不支持类型" + value.getClass().getSimpleName() + " 到BigDecimal的转换");
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        validateColumnLabel(columnLabel);
        Object value = getObject(columnLabel);

        if (value == null) {
            wasNull = true;
            return null;
        }

        wasNull = false;

        return this.validBigDecimal(value);
    }

    public boolean isBeforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
        // return false;
    }

    public boolean isAfterLast() throws SQLException {

        // return false;
        throw new UnsupportedOperationException();
    }

    public boolean isFirst() throws SQLException {

        // return false;
        throw new UnsupportedOperationException();
    }

    public boolean isLast() throws SQLException {

        // return false;
        throw new UnsupportedOperationException();
    }

    public void beforeFirst() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void afterLast() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public boolean first() throws SQLException {

        // return false;
        throw new UnsupportedOperationException();
    }

    public boolean last() throws SQLException {

        // return false;
        throw new UnsupportedOperationException();
    }

    public int getRow() throws SQLException {

        throw new UnsupportedOperationException();
        // return 0;
    }

    public boolean absolute(int row) throws SQLException {

        // return false;
        throw new UnsupportedOperationException();
    }

    public boolean relative(int rows) throws SQLException {

        // return false;
        throw new UnsupportedOperationException();
    }

    public boolean previous() throws SQLException {

        throw new UnsupportedOperationException();

    }

    public void setFetchDirection(int direction) throws SQLException {
        return;
    }

    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
        // return 0;
    }

    public void setFetchSize(int rows) throws SQLException {
        return;
    }

    public int getFetchSize() throws SQLException {
        return 0;
    }

    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    public boolean rowUpdated() throws SQLException {

        // return false;
        throw new UnsupportedOperationException();
    }

    public boolean rowInserted() throws SQLException {

        // return false;
        throw new UnsupportedOperationException();
    }

    public boolean rowDeleted() throws SQLException {

        // return false;
        throw new UnsupportedOperationException();
    }

    public void updateNull(int columnIndex) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void updateShort(int columnIndex, short x) throws SQLException {

    }

    public void updateInt(int columnIndex, int x) throws SQLException {

    }

    public void updateLong(int columnIndex, long x) throws SQLException {

    }

    public void updateFloat(int columnIndex, float x) throws SQLException {

    }

    public void updateDouble(int columnIndex, double x) throws SQLException {

    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

    }

    public void updateString(int columnIndex, String x) throws SQLException {

    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {

    }

    public void updateDate(int columnIndex, Date x) throws SQLException {

    }

    public void updateTime(int columnIndex, Time x) throws SQLException {

    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

    }

    public void updateObject(int columnIndex, Object x) throws SQLException {

    }

    public void updateNull(String columnLabel) throws SQLException {

    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {

    }

    public void updateByte(String columnLabel, byte x) throws SQLException {

    }

    public void updateShort(String columnLabel, short x) throws SQLException {

    }

    public void updateInt(String columnLabel, int x) throws SQLException {

    }

    public void updateLong(String columnLabel, long x) throws SQLException {

    }

    public void updateFloat(String columnLabel, float x) throws SQLException {

    }

    public void updateDouble(String columnLabel, double x) throws SQLException {

    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

    }

    public void updateString(String columnLabel, String x) throws SQLException {

    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {

    }

    public void updateDate(String columnLabel, Date x) throws SQLException {

    }

    public void updateTime(String columnLabel, Time x) throws SQLException {

    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

    }

    public void updateObject(String columnLabel, Object x) throws SQLException {

    }

    public void insertRow() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void updateRow() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void deleteRow() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void refreshRow() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void cancelRowUpdates() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void moveToInsertRow() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void moveToCurrentRow() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public Statement getStatement() throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Ref getRef(int columnIndex) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(int columnIndex) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Clob getClob(int columnIndex) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Array getArray(int columnIndex) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Ref getRef(String columnLabel) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(String columnLabel) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Clob getClob(String columnLabel) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Array getArray(String columnLabel) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        Timestamp ts = getTimestamp(columnIndex);
        if (ts == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        cal.setTimeInMillis(ts.getTime());
        return new Time(cal.getTimeInMillis());
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        Timestamp ts = getTimestamp(columnLabel);
        if (ts == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        cal.setTimeInMillis(ts.getTime());
        return new Time(cal.getTimeInMillis());
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        Timestamp ts = getTimestamp(columnIndex);
        if (ts == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        cal.setTimeInMillis(ts.getTime());
        // return null;
        return new Timestamp(cal.getTimeInMillis());

    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        Timestamp ts = getTimestamp(columnLabel);
        if (ts == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        cal.setTimeInMillis(ts.getTime());
        return new Timestamp(cal.getTimeInMillis());
    }

    public URL getURL(int columnIndex) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public URL getURL(String columnLabel) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    public RowId getRowId(int columnIndex) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public RowId getRowId(String columnLabel) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {

        throw new UnsupportedOperationException();

    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {

        throw new UnsupportedOperationException();

    }

    public int getHoldability() throws SQLException {

        // return 0;
        throw new UnsupportedOperationException();
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {

        throw new UnsupportedOperationException();

    }

    public void updateNString(String columnLabel, String nString) throws SQLException {

        throw new UnsupportedOperationException();

    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    public NClob getNClob(int columnIndex) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public NClob getNClob(String columnLabel) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    public String getNString(int columnIndex) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public String getNString(String columnLabel) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {

        // return null;
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    // FIXME 列不存在的时候不应该返回null，应该抛出列不存在异常才对
    // private static Object get(KVPair kv, String column){
    // if(kv.getKey()!=null){
    // Object o = kv.getKey().getIngoreTableName(column);
    // if(null!=o){
    // return o;
    // }
    // }
    // if(kv.getValue()!=null){
    // return kv.getValue().getIngoreTableName(column);
    // }
    // return null;
    // }
}
