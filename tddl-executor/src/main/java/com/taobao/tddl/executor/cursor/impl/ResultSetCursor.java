package com.taobao.tddl.executor.cursor.impl;

import java.sql.ResultSet;
import java.util.Map;

import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;

public class ResultSetCursor extends ResultCursor {

    private ResultSet rs;

    public ResultSetCursor(ResultSet rs){
        super((ISchematicCursor) null, (Map) null);
        this.rs = rs;
    }

    public ResultSet getResultSet() {
        return this.rs;
    }

}
