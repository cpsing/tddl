package com.taobao.tddl.optimizer.core.datatype;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;

import com.taobao.tddl.common.model.BaseRowSet;

public interface DataType extends Comparator<Object> {

    Object add(Object o1, Object o2);

    public static interface ResultGetter {

        Object get(ResultSet rs, int index) throws SQLException;

        Object get(BaseRowSet rs, int index);
    }

    ResultGetter getResultGetter();
}
