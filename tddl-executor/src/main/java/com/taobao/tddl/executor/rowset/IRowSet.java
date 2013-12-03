package com.taobao.tddl.executor.rowset;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

/**
 * 数据的核心接口，类似ResultSet一样的接口。下面有可能有join的实现，也可能有普通query或Merge实现。
 * 
 * @author Whisper
 */
public interface IRowSet {

    Object getObject(int index);

    void setObject(int index, Object value);

    Integer getInteger(int index);

    void setInteger(int index, Integer value);

    Long getLong(int index);

    void setLong(int index, Long value);

    public List<Object> getValues();

    public String getString(int index);

    public void setString(int index, String str);

    public Boolean getBoolean(int index);

    public void setBoolean(int index, Boolean bool);

    public Short getShort(int index);

    public void setShort(int index, Short shortval);

    public Float getFloat(int index);

    public void setFloat(int index, Float fl);

    public Double getDouble(int index);

    public void setDouble(int index, Double doub);

    public byte[] getBytes(int index);

    public void setBytes(int index, byte[] bytes);

    public Date getDate(int index);

    public void setDate(int index, Date date);

    public Timestamp getTimestamp(int index);

    public void setTimestamp(int index, Timestamp timestamp);

    ICursorMeta getParentCursorMeta();
}
