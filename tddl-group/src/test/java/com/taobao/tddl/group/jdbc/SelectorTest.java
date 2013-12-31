package com.taobao.tddl.group.jdbc;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.tddl.common.mock.MockDataSource;
import com.taobao.tddl.common.mock.MockDataSource.ExecuteInfo;
import com.taobao.tddl.common.model.DBType;

public class SelectorTest {

    @Before
    public void setUp() {
        MockDataSource.clearTrace();
    }

    @Test
    public void test_读写分离() {
        TGroupDataSource tgds = new TGroupDataSource();
        tgds.setDbGroupKey("dbKey0");
        List<DataSourceWrapper> dataSourceWrappers = new ArrayList<DataSourceWrapper>();
        MockDataSource db1 = new MockDataSource("db", "db1");
        MockDataSource db2 = new MockDataSource("db", "db2");
        DataSourceWrapper dsw1 = new DataSourceWrapper("db1", "w", db1, DBType.MYSQL);
        DataSourceWrapper dsw2 = new DataSourceWrapper("db2", "r", db2, DBType.MYSQL);
        dataSourceWrappers.add(dsw1);
        dataSourceWrappers.add(dsw2);
        tgds.init(dataSourceWrappers);

        TGroupConnection conn = null;
        Statement stat = null;
        try {
            conn = tgds.getConnection();
            stat = conn.createStatement();
            stat.executeUpdate("update xxx set name = 'newname'");
            stat.executeQuery("select 1 from test");
            MockDataSource.showTrace();
            Assert.assertTrue(MockDataSource.hasTrace("db", "db1", "update xxx set name = 'newname'"));
            Assert.assertTrue(MockDataSource.hasTrace("db", "db2", "select 1 from test"));
        } catch (SQLException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
                if (stat != null) {
                    try {
                        stat.close();
                    } catch (SQLException e) {
                    }
                }
            }
        }
    }

    @Test
    public void test_相同权重() {
        TGroupDataSource tgds = new TGroupDataSource();
        tgds.setDbGroupKey("dbKey0");
        List<DataSourceWrapper> dataSourceWrappers = new ArrayList<DataSourceWrapper>();
        MockDataSource db1 = new MockDataSource("db", "db1");
        MockDataSource db2 = new MockDataSource("db", "db2");
        DataSourceWrapper dsw1 = new DataSourceWrapper("db1", "r20", db1, DBType.MYSQL);
        DataSourceWrapper dsw2 = new DataSourceWrapper("db2", "r20", db2, DBType.MYSQL);
        dataSourceWrappers.add(dsw1);
        dataSourceWrappers.add(dsw2);
        tgds.init(dataSourceWrappers);

        TGroupConnection conn = null;
        Statement stat = null;
        try {
            for (int i = 0; i < 1000; i++) {
                conn = tgds.getConnection();
                stat = conn.createStatement();
                stat.executeQuery("select 1 from test");
            }

            int count1 = 0;
            int count2 = 0;
            List<ExecuteInfo> infos = MockDataSource.getTrace();
            for (ExecuteInfo info : infos) {
                if (info.sql != null && info.sql.startsWith("select 1 from test")) {
                    if ("db1".equalsIgnoreCase(info.ds.getName())) {
                        count1++;
                    } else {
                        count2++;
                    }
                }
            }

            Assert.assertTrue(count2 > 0 && count1 > 0);
        } catch (SQLException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
                if (stat != null) {
                    try {
                        stat.close();
                    } catch (SQLException e) {
                    }
                }
            }
        }
    }

    @Test
    public void test_不同读优先级() {
        TGroupDataSource tgds = new TGroupDataSource();
        tgds.setDbGroupKey("dbKey0");
        List<DataSourceWrapper> dataSourceWrappers = new ArrayList<DataSourceWrapper>();
        MockDataSource db1 = new MockDataSource("db", "db1");
        MockDataSource db2 = new MockDataSource("db", "db2");
        DataSourceWrapper dsw1 = new DataSourceWrapper("db1", "r20p20", db1, DBType.MYSQL);
        DataSourceWrapper dsw2 = new DataSourceWrapper("db2", "r20p10", db2, DBType.MYSQL);
        dataSourceWrappers.add(dsw1);
        dataSourceWrappers.add(dsw2);
        tgds.init(dataSourceWrappers);

        TGroupConnection conn = null;
        Statement stat = null;
        try {
            for (int i = 0; i < 1000; i++) {
                conn = tgds.getConnection();
                stat = conn.createStatement();
                stat.executeQuery("select 1 from test");
            }

            int count1 = 0;
            int count2 = 0;
            List<ExecuteInfo> infos = MockDataSource.getTrace();
            for (ExecuteInfo info : infos) {
                if (info.sql != null && info.sql.startsWith("select 1 from test")) {
                    if ("db1".equalsIgnoreCase(info.ds.getName())) {
                        count1++;
                    } else {
                        count2++;
                    }
                }
            }

            Assert.assertTrue(count1 == 1000); // 全部选择1库
            Assert.assertTrue(count2 == 0);
        } catch (SQLException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
                if (stat != null) {
                    try {
                        stat.close();
                    } catch (SQLException e) {
                    }
                }
            }
        }
    }

    @Test
    public void test_不同写优先级() {
        TGroupDataSource tgds = new TGroupDataSource();
        tgds.setDbGroupKey("dbKey0");
        List<DataSourceWrapper> dataSourceWrappers = new ArrayList<DataSourceWrapper>();
        MockDataSource db1 = new MockDataSource("db", "db1");
        MockDataSource db2 = new MockDataSource("db", "db2");
        DataSourceWrapper dsw1 = new DataSourceWrapper("db1", "w20q20", db1, DBType.MYSQL);
        DataSourceWrapper dsw2 = new DataSourceWrapper("db2", "w20q10", db2, DBType.MYSQL);
        dataSourceWrappers.add(dsw1);
        dataSourceWrappers.add(dsw2);
        tgds.init(dataSourceWrappers);

        TGroupConnection conn = null;
        Statement stat = null;
        try {
            for (int i = 0; i < 1000; i++) {
                conn = tgds.getConnection();
                stat = conn.createStatement();
                stat.executeQuery("update xxx set name = 'newname'");
            }

            int count1 = 0;
            int count2 = 0;
            List<ExecuteInfo> infos = MockDataSource.getTrace();
            for (ExecuteInfo info : infos) {
                if (info.sql != null && info.sql.startsWith("update xxx set name = 'newname'")) {
                    if ("db1".equalsIgnoreCase(info.ds.getName())) {
                        count1++;
                    } else {
                        count2++;
                    }
                }
            }

            Assert.assertTrue(count1 == 1000); // 全部选择1库
            Assert.assertTrue(count2 == 0);
        } catch (SQLException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
                if (stat != null) {
                    try {
                        stat.close();
                    } catch (SQLException e) {
                    }
                }
            }
        }
    }
}
