package com.taobao.tddl.group;

import java.sql.Connection;
import java.sql.Statement;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;

import com.alibaba.druid.pool.DruidDataSource;
import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.group.jdbc.DataSourceWrapper;
import com.taobao.tddl.group.jdbc.TGroupDataSource;

public class BaseGroupTest {

    @Before
    public void beforeClass() {
        MockServer.setUpMockServer();
    }

    @After
    public void after() {
        MockServer.tearDownMockServer();
    }

    public static DataSource getMySQLDataSource() {
        return getMySQLDataSource(1);
    }

    public static DataSource getMySQLDataSource(int num) {
        if (num > 3) {
            num = 1;
        }
        DruidDataSource ds = new DruidDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername("tddl");
        ds.setPassword("tddl");
        ds.setUrl("jdbc:mysql://10.232.31.154/tddl_sample_" + (num - 1));
        return ds;

    }

    // 删除三个库中crud表的所有记录
    public static void deleteAll() throws Exception {
        DataSource ds1 = getMySQLDataSource(1);
        DataSource ds2 = getMySQLDataSource(2);
        DataSource ds3 = getMySQLDataSource(3);

        Connection conn = null;
        Statement stmt = null;

        TGroupDataSource ds = new TGroupDataSource();
        DataSourceWrapper dsw = new DataSourceWrapper("tddl_sample_0", "rw", ds1, DBType.MYSQL);
        ds.init(dsw);

        conn = ds.getConnection();
        stmt = conn.createStatement();
        stmt.executeUpdate("delete from tddl_test_0000");
        stmt.close();
        conn.close();

        ds = new TGroupDataSource();
        dsw = new DataSourceWrapper("tddl_sample_1", "rw", ds2, DBType.MYSQL);
        ds.init(dsw);
        conn = ds.getConnection();
        stmt = conn.createStatement();
        stmt.executeUpdate("delete from tddl_test_0000");
        stmt.close();
        conn.close();

        ds = new TGroupDataSource();
        dsw = new DataSourceWrapper("tddl_sample_2", "rw", ds3, DBType.MYSQL);
        ds.init(dsw);
        conn = ds.getConnection();
        stmt = conn.createStatement();
        stmt.executeUpdate("delete from tddl_test_0000");
        stmt.close();
        conn.close();
    }
}
