package com.taobao.tddl.executor.rowset;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @author mengshi.sunmengshi 2013-12-3 上午11:06:04
 * @since 5.0.0
 */
public abstract class AbstractRowSet implements IRowSet {

    private final ICursorMeta iCursorMeta;

    public AbstractRowSet(ICursorMeta iCursorMeta){
        super();
        this.iCursorMeta = iCursorMeta;
    }

    @Override
    public ICursorMeta getParentCursorMeta() {
        return iCursorMeta;
    }

    @Override
    public Integer getInteger(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return DataType.IntegerType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public void setInteger(int index, Integer value) {
        setObject(index, value);
    }

    @Override
    public Long getLong(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return DataType.LongType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public void setLong(int index, Long value) {
        setObject(index, value);
    }

    @Override
    public String getString(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return DataType.StringType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public void setString(int index, String str) {
        setObject(index, str);
    }

    @Override
    public Boolean getBoolean(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return DataType.BooleanType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public void setBoolean(int index, Boolean bool) {
        setObject(index, bool);
    }

    @Override
    public Short getShort(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return DataType.ShortType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public void setShort(int index, Short shortval) {
        setObject(index, shortval);
    }

    @Override
    public Float getFloat(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return DataType.FloatType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public void setFloat(int index, Float fl) {
        setObject(index, fl);
    }

    @Override
    public Double getDouble(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return DataType.DoubleType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public void setDouble(int index, Double doub) {
        setObject(index, doub);
    }

    @Override
    public byte[] getBytes(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return DataType.BytesType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public void setBytes(int index, byte[] bytes) {
        setObject(index, bytes);
    }

    @Override
    public Date getDate(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return DataType.DateType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public void setDate(int index, Date date) {
        setObject(index, date);
    }

    @Override
    public Timestamp getTimestamp(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return DataType.TimestampType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public void setTimestamp(int index, Timestamp timestamp) {
        setObject(index, timestamp);
    }

    @Override
    public Time getTime(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return DataType.TimeType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public void setTime(int index, Time time) {
        setObject(index, time);
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return DataType.BigDecimalType.convertFrom(cm.getDataType().getResultGetter().get(this, index));
    }

    @Override
    public void setBigDecimal(int index, BigDecimal bigDecimal) {
        setObject(index, bigDecimal);
    }

    @Override
    public List<Object> getValues() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (ColumnMeta cm : this.getParentCursorMeta().getColumns()) {
            int index = this.getParentCursorMeta().getIndex(cm.getTableName(), cm.getName());
            sb.append(cm.getName() + ":" + this.getValues().get(index) + " ");
        }
        return sb.toString();
    }
}
