package com.taobao.tddl.common.mock;

import java.sql.ResultSet;

public interface ExecuteHandler {

    public ResultSet execute(String method, String sql);

    public boolean executeSql(String method, String sql);
}
