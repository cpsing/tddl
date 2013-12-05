package com.taobao.tddl.executor.cursor.impl;

import java.sql.ResultSet;

import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;

public class ResultSetCursor extends ResultCursor {

    private ResultSet rs;

    public ResultSetCursor(ResultSet rs){
        super((ISchematicCursor) null, null);
        this.rs = rs;
    }

    public ResultSet getResultSet() {
        return this.rs;
    }

}
