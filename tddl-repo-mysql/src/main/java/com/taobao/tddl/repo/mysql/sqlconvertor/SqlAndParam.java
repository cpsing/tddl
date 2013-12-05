package com.taobao.tddl.repo.mysql.sqlconvertor;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;

public class SqlAndParam {

    public String                         sql;
    public Map<Integer, ParameterContext> param;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("sql: ").append(sql).append("\n").append("param: ").append(param).append("\n");
        return sb.toString();
    }
}
