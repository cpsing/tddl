package com.taobao.tddl.atom.jdbc;

public class SqlMetaDataFactory {

    public static SqlMetaDataImpl getSqlMetaData(String sql, String... logicTables) {
        SqlMetaDataImpl sqlMetaData = new SqlMetaDataImpl();
        sqlMetaData.setOriSql(sql);
        sqlMetaData.setSqlBuilder(new StringBuilder(sql));
        sqlMetaData.addLogicTables(logicTables);
        sqlMetaData.setParsed(false);
        return sqlMetaData;
    }

}
