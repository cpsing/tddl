package com.taobao.tddl.executor.rowset;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import com.taobao.tddl.executor.cursor.ICursorMeta;

/**
 * 可以用来给列改名，去除一个列
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午11:05:57
 * @since 5.0.0
 */
public class RowSetWrapper extends AbstractRowSet implements IRowSet {

    protected final ICursorMeta newCursorMeta;
    protected IRowSet           parentRowSet;

    public RowSetWrapper(ICursorMeta iCursorMeta, IRowSet rowSet){
        super(iCursorMeta);
        this.newCursorMeta = iCursorMeta;
        this.parentRowSet = rowSet;
    }

    public Object getObject(int index) {
        return parentRowSet.getObject(index);
    }

    public void setObject(int index, Object value) {
        parentRowSet.setObject(index, value);
    }

    public Integer getInteger(int index) {
        return parentRowSet.getInteger(index);
    }

    public void setInteger(int index, Integer value) {
        parentRowSet.setInteger(index, value);
    }

    public Long getLong(int index) {
        return parentRowSet.getLong(index);
    }

    public void setLong(int index, Long value) {
        parentRowSet.setLong(index, value);
    }

    public List<Object> getValues() {
        return parentRowSet.getValues();
    }

    public ICursorMeta getParentCursorMeta() {
        return newCursorMeta;
    }

    public String getString(int index) {
        return parentRowSet.getString(index);
    }

    public void setString(int index, String str) {
        parentRowSet.setString(index, str);
    }

    public Boolean getBoolean(int index) {
        return parentRowSet.getBoolean(index);
    }

    public void setBoolean(int index, Boolean bool) {
        parentRowSet.setBoolean(index, bool);
    }

    public Short getShort(int index) {
        return parentRowSet.getShort(index);
    }

    public void setShort(int index, Short shortval) {
        parentRowSet.setShort(index, shortval);
    }

    public Float getFloat(int index) {
        return parentRowSet.getFloat(index);
    }

    public void setFloat(int index, Float fl) {
        parentRowSet.setFloat(index, fl);
    }

    public Double getDouble(int index) {
        return parentRowSet.getDouble(index);
    }

    public void setDouble(int index, Double doub) {
        parentRowSet.setDouble(index, doub);
    }

    public byte[] getBytes(int index) {
        return parentRowSet.getBytes(index);
    }

    public void setBytes(int index, byte[] bytes) {
        parentRowSet.setBytes(index, bytes);
    }

    public Date getDate(int index) {
        return parentRowSet.getDate(index);
    }

    public void setDate(int index, Date date) {
        parentRowSet.setDate(index, date);
    }

    public Timestamp getTimestamp(int index) {
        return parentRowSet.getTimestamp(index);
    }

    public void setTimestamp(int index, Timestamp timestamp) {
        parentRowSet.setTimestamp(index, timestamp);
    }

    public IRowSet getParentRowSet() {
        return parentRowSet;
    }

    public void setParentRowSet(IRowSet parentRowSet) {
        this.parentRowSet = parentRowSet;
    }

}
