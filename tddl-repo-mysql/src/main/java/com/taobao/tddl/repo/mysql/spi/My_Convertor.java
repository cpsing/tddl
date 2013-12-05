package com.taobao.tddl.repo.mysql.spi;

import java.sql.ResultSet;

import com.taobao.tddl.executor.common.ICursorMeta;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.rowset.ResultSetRowSet;

public class My_Convertor {

    public static IRowSet convert(ResultSet rs, ICursorMeta meta) {
        IRowSet rowSet = new ResultSetRowSet(meta, rs);
        // int i = 1;
        // for (; i <= size; i++) {
        // try {
        // Object obValue = rs.getObject(i);
        // if (obValue instanceof BigDecimal) {
        // obValue = ((BigDecimal) obValue).longValue();
        // } else if (obValue instanceof BigInteger) {
        // obValue = ((BigInteger) obValue).longValue();
        // }
        // rowSet.setObject(i-1, obValue);
        // } catch (SQLException e) {
        // e.printStackTrace();
        // }
        // }
        return rowSet;
    }
}
