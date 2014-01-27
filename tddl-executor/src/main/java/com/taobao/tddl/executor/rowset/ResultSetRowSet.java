package com.taobao.tddl.executor.rowset;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.executor.cursor.ICursorMeta;

/**
 * @author mengshi.sunmengshi 2013-12-3 上午11:06:10
 * @since 5.0.0
 */
public class ResultSetRowSet extends AbstractRowSet implements IRowSet {

    ResultSet rs = null;

    public ResultSetRowSet(ICursorMeta meta, ResultSet rs){
        super(meta);
        this.rs = rs;
    }

    @Override
    public Object getObject(int index) {
        try {
            int actIndex = index + 1;
            Object obValue = rs.getObject(actIndex);
            // if (obValue instanceof BigDecimal) {
            // // obValue = ((BigDecimal) obValue).longValue();
            // } else if (obValue instanceof BigInteger) {
            // obValue = ((BigInteger) obValue).longValue();
            // }
            //
            // // Date和Time要做转换，防止丢掉了时间或者日期部分
            // if (obValue instanceof Date || (obValue instanceof Time)) {
            // obValue = rs.getTimestamp(actIndex);
            // }
            if (rs.wasNull()) {
                return null;
            }
            return obValue;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setObject(int index, Object value) {
        throw new RuntimeException("不允许设置mysql的列值");
    }

    @Override
    public List<Object> getValues() {
        List<Object> res = new ArrayList<Object>();
        for (int i = 0; i < getParentCursorMeta().getColumns().size(); i++) {
            // Object obValue = rs.getObject(i);
            // if (obValue instanceof BigDecimal) {
            // obValue = ((BigDecimal) obValue).longValue();
            // } else if (obValue instanceof BigInteger) {
            // obValue = ((BigInteger) obValue).longValue();
            // }
            // if(obValue instanceof Date || (obValue instanceof Time))
            // {
            // obValue = rs.getTimestamp(i);
            // }
            res.add(this.getObject(i));
        }
        return res;
    }

    @Override
    public Boolean getBoolean(int index) {
        try {
            int actIndex = index + 1;
            boolean bool = rs.getBoolean(actIndex);
            if (rs.wasNull()) {
                return null;
            }
            return bool;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Date getDate(int index) {
        try {
            int actIndex = index + 1;
            return rs.getDate(actIndex);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Double getDouble(int index) {
        try {
            int actIndex = index + 1;
            double d = rs.getDouble(actIndex);
            if (rs.wasNull()) {
                return null;
            } else {
                return d;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Float getFloat(int index) {
        try {
            int actIndex = index + 1;
            float f = rs.getFloat(actIndex);
            if (rs.wasNull()) {
                return null;
            } else {
                return f;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer getInteger(int index) {
        try {
            int actIndex = index + 1;
            int inte = rs.getInt(actIndex);
            if (rs.wasNull()) {
                return null;
            }
            return inte;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long getLong(int index) {
        try {
            int actIndex = index + 1;
            long l = rs.getLong(actIndex);
            if (rs.wasNull()) {
                return null;
            }
            return l;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Short getShort(int index) {
        try {
            int actIndex = index + 1;
            short s = rs.getShort(actIndex);
            if (rs.wasNull()) {
                return null;
            }
            return s;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getString(int index) {
        try {
            int actIndex = index + 1;
            return rs.getString(actIndex);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Timestamp getTimestamp(int index) {
        try {
            int actIndex = index + 1;
            return rs.getTimestamp(actIndex);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] getBytes(int index) {
        try {
            int actIndex = index + 1;
            return rs.getBytes(actIndex);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
