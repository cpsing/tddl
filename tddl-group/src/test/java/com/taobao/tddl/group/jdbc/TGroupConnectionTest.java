package com.taobao.tddl.group.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.tddl.common.mock.MockDataSource;
import com.taobao.tddl.common.model.DBType;

/**
 * @author yangzhu
 */
public class TGroupConnectionTest {

    @Before
    public void setUp() {
        MockDataSource.clearTrace();
    }

    @Test
    public void java_sql_Connection_api_support() throws Exception {
        TGroupDataSource ds = new TGroupDataSource();

        Connection conn = ds.getConnection();
        assertFalse(conn.isClosed());
        assertTrue(conn.getAutoCommit());
        assertNull(conn.getWarnings());
        assertTrue((conn.getMetaData() instanceof TGroupDatabaseMetaData));

        assertTrue((conn.createStatement() instanceof TGroupStatement));
        assertTrue((conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE) instanceof TGroupStatement));
        assertTrue((conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE,
            ResultSet.HOLD_CURSORS_OVER_COMMIT) instanceof TGroupStatement));

        assertTrue((conn.prepareStatement("sql") instanceof TGroupPreparedStatement));
        assertTrue((conn.prepareStatement("sql", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE) instanceof TGroupPreparedStatement));
        assertTrue((conn.prepareStatement("sql",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE,
            ResultSet.HOLD_CURSORS_OVER_COMMIT) instanceof TGroupPreparedStatement));
        assertTrue((conn.prepareStatement("sql", Statement.RETURN_GENERATED_KEYS) instanceof TGroupPreparedStatement));
        assertTrue((conn.prepareStatement("sql", new int[0]) instanceof TGroupPreparedStatement));
        assertTrue((conn.prepareStatement("sql", new String[0]) instanceof TGroupPreparedStatement));

        // assertTrue((conn.prepareCall("sql") instanceof
        // TGroupCallableStatement));
        // assertTrue((conn.prepareCall("sql",
        // ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)
        // instanceof TGroupCallableStatement));
        // assertTrue((conn.prepareCall("sql",
        // ResultSet.TYPE_SCROLL_INSENSITIVE,
        // ResultSet.CONCUR_UPDATABLE,
        // ResultSet.HOLD_CURSORS_OVER_COMMIT) instanceof
        // TGroupCallableStatement));
    }

    @Test
    public void test_一个连接上创建两个Statement() {
        TGroupDataSource tgds = new TGroupDataSource();
        tgds.setDbGroupKey("dbKey0");
        List<DataSourceWrapper> dataSourceWrappers = new ArrayList<DataSourceWrapper>();
        MockDataSource db1 = new MockDataSource("db", "db1");
        MockDataSource db2 = new MockDataSource("db", "db2");
        DataSourceWrapper dsw1 = new DataSourceWrapper("db1", "rw", db1, DBType.MYSQL);
        DataSourceWrapper dsw2 = new DataSourceWrapper("db2", "r", db2, DBType.MYSQL);
        dataSourceWrappers.add(dsw1);
        dataSourceWrappers.add(dsw2);
        tgds.init(dataSourceWrappers);

        TGroupConnection conn = null;
        Statement stat = null;
        try {
            db1.setClosed(true);
            db2.setClosed(false);
            // 链接一旦选定就会保持所选择的库，直到close
            conn = tgds.getConnection();
            stat = conn.createStatement();
            stat.executeQuery("select 1 from test");
            MockDataSource.showTrace();
            Assert.assertTrue(MockDataSource.hasTrace("db", "db2", "select 1 from test"));

            db1.setClosed(false);
            db2.setClosed(true);
            stat = conn.createStatement();
            stat.executeQuery("select 2 from test");
            // Assert.assertTrue(MockDataSource.hasTrace("db", "db1",
            // "select 1 from test"));
            Assert.fail("没有重用第一个连接");
        } catch (SQLException e) {
            // Assert.fail(ExceptionUtils.getFullStackTrace(e));
            System.out.println(e.getMessage());
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
    public void test_创建Statement失败重试_读请求() {
        TGroupDataSource tgds = new TGroupDataSource();
        tgds.setDbGroupKey("dbKey0");
        List<DataSourceWrapper> dataSourceWrappers = new ArrayList<DataSourceWrapper>();
        MockDataSource db1 = new MockDataSource("db", "db1");
        MockDataSource db2 = new MockDataSource("db", "db2");
        DataSourceWrapper dsw1 = new DataSourceWrapper("db1", "rw", db1, DBType.MYSQL);
        DataSourceWrapper dsw2 = new DataSourceWrapper("db2", "r", db2, DBType.MYSQL);
        dataSourceWrappers.add(dsw1);
        dataSourceWrappers.add(dsw2);
        tgds.init(dataSourceWrappers);

        TGroupConnection conn = null;
        Statement stat = null;
        try {
            conn = tgds.getConnection();
            stat = conn.createStatement();
            MockDataSource.addPreException(MockDataSource.m_createStatement, db1.genFatalSQLException());
            stat.executeQuery("select 1 from test");
            MockDataSource.showTrace();
            Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "getConnection"));
            // 会在db2做重试
            Assert.assertTrue(MockDataSource.hasMethod("db", "db2", "getConnection"));
        } catch (SQLException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
            // System.out.println(e.getMessage());
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
    public void test_创建Statement失败不重试_写请求() {
        TGroupDataSource tgds = new TGroupDataSource();
        tgds.setDbGroupKey("dbKey0");
        List<DataSourceWrapper> dataSourceWrappers = new ArrayList<DataSourceWrapper>();
        MockDataSource db1 = new MockDataSource("db", "db1");
        MockDataSource db2 = new MockDataSource("db", "db2");
        DataSourceWrapper dsw1 = new DataSourceWrapper("db1", "rw", db1, DBType.MYSQL);
        DataSourceWrapper dsw2 = new DataSourceWrapper("db2", "r", db2, DBType.MYSQL);
        dataSourceWrappers.add(dsw1);
        dataSourceWrappers.add(dsw2);
        tgds.init(dataSourceWrappers);

        TGroupConnection conn = null;
        Statement stat = null;
        try {
            conn = tgds.getConnection();
            stat = conn.createStatement();
            MockDataSource.addPreException(MockDataSource.m_createStatement, db1.genFatalSQLException());
            stat.executeQuery("update test set name = 'newname'");
        } catch (SQLException e) {
            // Assert.fail(ExceptionUtils.getFullStackTrace(e));
            System.out.println(e.getMessage());
            Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "getConnection"));
            // 写操作不会在db2做重试
            Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "getConnection"));
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
    public void test_autocommit() {
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
            stat.executeQuery("select 1 from test");
            conn.setAutoCommit(false);
            stat.executeUpdate("update t set name='newName'");
            conn.commit();
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
        MockDataSource.showTrace();
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "getConnection"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db2", "getConnection"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "setAutoCommit"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db1", "rollback"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "setAutoCommit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "rollback"));
    }

    @Test
    public void test_no_trans() {
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
            stat.executeQuery("select 1 from test");
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
        MockDataSource.showTrace();
        Assert.assertFalse(MockDataSource.hasMethod("db", "db1", "getConnection"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db2", "getConnection"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db1", "setAutoCommit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db1", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db1", "rollback"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "setAutoCommit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "rollback"));
    }

    @Test
    public void test_write_trans() {
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
            conn.setAutoCommit(false);
            stat.executeUpdate("update t set name='newName'");
            conn.commit();
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
        MockDataSource.showTrace();
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "getConnection"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "getConnection"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "setAutoCommit"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db1", "rollback"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "setAutoCommit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "rollback"));
    }

    @Test
    public void test_read_trans() {
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
            conn.setAutoCommit(false);
            stat.executeQuery("select 1 from test");
            conn.commit();
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
        MockDataSource.showTrace();
        // 如果是事务读，强制选择了写库
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "getConnection"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "getConnection"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "setAutoCommit"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db1", "rollback"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "setAutoCommit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "rollback"));
    }

    @Test
    public void test_write_and_read_trans() {
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
            conn.setAutoCommit(false);
            stat.executeQuery("update t set name='newName'");
            stat.executeQuery("select 1 from test");
            conn.commit();
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
        MockDataSource.showTrace();
        // 事务中的读请求，选择写库
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "getConnection"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "getConnection"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "setAutoCommit"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db1", "rollback"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "setAutoCommit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "rollback"));
    }

    @Test
    public void test_read_and_write_trans() {
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
            conn.setAutoCommit(false);
            stat.executeQuery("select 1 from test");
            stat.executeQuery("update t set name='newName'");
            conn.commit();
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
        MockDataSource.showTrace();
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "getConnection"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "getConnection"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "setAutoCommit"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db1", "rollback"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "setAutoCommit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "rollback"));
    }

    @Test
    public void test_read_untrans_write_trans() {
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
            stat.executeQuery("select 1 from test");
            conn.setAutoCommit(false);
            stat.executeQuery("update t set name='newName'");
            conn.commit();
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
        MockDataSource.showTrace();
        // 刚开始读不处于事务中，选择读请求
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "getConnection"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db2", "getConnection"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "setAutoCommit"));
        Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db1", "rollback"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "setAutoCommit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "commit"));
        Assert.assertFalse(MockDataSource.hasMethod("db", "db2", "rollback"));
    }
}
