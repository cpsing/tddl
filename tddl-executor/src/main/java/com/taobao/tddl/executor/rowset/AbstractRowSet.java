package com.taobao.tddl.executor.rowset;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;

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
        return (Integer) cm.getDataType().getResultGetter().get(this, index);

        // Object val = this.getObject(index);
        // if (val == null) return 0;
        //
        // if (val instanceof Integer) return (Integer) val;
        //
        // if (val instanceof Number) return ((Number) val).intValue();
        //
        // if (val instanceof BigDecimal) return ((BigDecimal) val).intValue();
        //
        // String strVal = this.getString(index);
        //
        // return Integer.valueOf(strVal);
    }

    @Override
    public void setInteger(int index, Integer value) {
        setObject(index, value);
    }

    @Override
    public Long getLong(int index) {

        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return (Long) cm.getDataType().getResultGetter().get(this, index);

        // Object val = this.getObject(index);
        // if (val == null) return 0L;
        //
        // if (val instanceof Long) return (Long) val;
        //
        // if (val instanceof Number) return ((Number) val).longValue();
        //
        // if (val instanceof BigDecimal) return ((BigDecimal) val).longValue();
        //
        // String strVal = this.getString(index);
        //
        // return Long.valueOf(strVal);
    }

    @Override
    public void setLong(int index, Long value) {
        setObject(index, value);
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

    @Override
    public String getString(int index) {
        return String.valueOf(getObject(index));
    }

    @Override
    public void setString(int index, String str) {
        setObject(index, str);
    }

    @Override
    public Boolean getBoolean(int index) {

        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return (Boolean) cm.getDataType().getResultGetter().get(this, index);

        // Object val = this.getObject(index);
        // // -1 || >0 是true
        // if (val == null) return false;
        //
        // if (val instanceof Boolean) return (Boolean) val;
        //
        // String strVal = this.getString(index);
        //
        // return Boolean.valueOf(strVal);
    }

    @Override
    public void setBoolean(int index, Boolean bool) {
        setObject(index, bool);
    }

    @Override
    public Short getShort(int index) {

        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return (Short) cm.getDataType().getResultGetter().get(this, index);

        // Object val = this.getObject(index);
        // if (val == null) return 0;
        //
        // if (val instanceof Short) return (Short) val;
        //
        // if (val instanceof Number) return ((Number) val).shortValue();
        //
        // if (val instanceof BigDecimal) return ((BigDecimal)
        // val).shortValue();
        //
        // String strVal = this.getString(index);
        //
        // return Short.valueOf(strVal);
    }

    @Override
    public void setShort(int index, Short shortval) {
        setObject(index, shortval);
    }

    @Override
    public Float getFloat(int index) {

        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return (Float) cm.getDataType().getResultGetter().get(this, index);

        // Object val = this.getObject(index);
        // if (val == null) return (float) 0.0;
        //
        // if (val instanceof Float) return (Float) val;
        //
        // if (val instanceof Number) return ((Number) val).floatValue();
        //
        // if (val instanceof BigDecimal) return ((BigDecimal)
        // val).floatValue();
        //
        // String strVal = this.getString(index);
        //
        // return Float.valueOf(strVal);
    }

    @Override
    public void setFloat(int index, Float fl) {
        setObject(index, fl);
    }

    @Override
    public Double getDouble(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return (Double) cm.getDataType().getResultGetter().get(this, index);

        // Object val = this.getObject(index);
        // if (val == null) return 0.0;
        //
        // if (val instanceof Double) return (Double) val;
        //
        // if (val instanceof Number) return ((Number) val).doubleValue();
        //
        // if (val instanceof BigDecimal) return ((BigDecimal)
        // val).doubleValue();
        //
        // String strVal = this.getString(index);
        //
        // return Double.valueOf(strVal);
    }

    @Override
    public void setDouble(int index, Double doub) {
        setObject(index, doub);
    }

    @Override
    public byte[] getBytes(int index) {

        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return (byte[]) cm.getDataType().getResultGetter().get(this, index);

        // Object obj = getObject(index);
        // if (obj == null) return null;
        // if (obj instanceof byte[]) return (byte[]) obj;
        // else throw new RuntimeException("暂不支持类型:" + obj.getClass() +
        // " 的getBytes操作");
    }

    @Override
    public void setBytes(int index, byte[] bytes) {
        setObject(index, bytes);
    }

    @Override
    public Date getDate(int index) {

        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return (Date) cm.getDataType().getResultGetter().get(this, index);

        // Object obj = getObject(index);
        // if (obj == null) return null;
        // if (obj instanceof Date) return (Date) obj;
        // if (obj instanceof java.util.Date) {
        // return new Date(((java.util.Date) obj).getTime());
        // } else throw new RuntimeException("暂不支持类型:" + obj.getClass() +
        // " 的getDate操作");
    }

    @Override
    public void setDate(int index, Date date) {
        setObject(index, date);
    }

    @Override
    public Timestamp getTimestamp(int index) {
        ColumnMeta cm = iCursorMeta.getColumns().get(index);
        return (Timestamp) cm.getDataType().getResultGetter().get(this, index);
        // Object obj = getObject(index);
        // if (obj == null) return null;
        // if (obj instanceof Timestamp) return (Timestamp) obj;
        // if (obj instanceof java.util.Date) {
        // return new Timestamp(((java.util.Date) obj).getTime());
        // } else throw new RuntimeException("暂不支持类型:" + obj.getClass() +
        // " 的getTimestamp操作");
    }

    @Override
    public void setTimestamp(int index, Timestamp timestamp) {
        setObject(index, timestamp);
    }

}
