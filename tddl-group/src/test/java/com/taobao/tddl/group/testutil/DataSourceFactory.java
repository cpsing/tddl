package com.taobao.tddl.group.testutil;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;
import com.taobao.tddl.common.mockdatasource.MockDataSource;

/**
 * @author yangzhu
 */
public class DataSourceFactory {

    public static DataSource getMySQLDataSource() {
        return getMySQLDataSource(1);
    }

    public static DataSource getMySQLDataSource(int num) {
        if (num > 3) num = 1;
        MockDataSource db1 = new MockDataSource("db", "group_test_" + num);
        return db1;

    }

    public static DataSource getLocalMySQLDataSource() {
        return getLocalMySQLDataSource(1);
    }

    public static DataSource getLocalMySQLDataSource(int num) {
        if (num > 3) num = 1;
        DruidDataSource ds = new DruidDataSource();
        // ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername("root");
        ds.setPassword("zhh");
        ds.setUrl("jdbc:mysql://localhost/group_test_" + num);
        return ds;

    }

    public static DataSource getOracleDataSource() {
        return null;
    }
}
