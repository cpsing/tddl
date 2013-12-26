package com.taobao.tddl.group.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.tddl.common.mock.MockDataSource;
import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.group.jdbc.DataSourceWrapper;
import com.taobao.tddl.group.jdbc.TGroupConnection;
import com.taobao.tddl.group.jdbc.TGroupDataSource;

public class TGroupStatementTest {

    private static TGroupDataSource tgds;
    private static MockDataSource   db1 = new MockDataSource("db", "db1");
    private static MockDataSource   db2 = new MockDataSource("db", "db2");

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        tgds = new TGroupDataSource();
        tgds.setDbGroupKey("dbKey0");
        List<DataSourceWrapper> dataSourceWrappers = new ArrayList<DataSourceWrapper>();
        DataSourceWrapper dsw1 = new DataSourceWrapper("db1", "rw", db1, DBType.MYSQL);
        DataSourceWrapper dsw2 = new DataSourceWrapper("db2", "r", db2, DBType.MYSQL);
        dataSourceWrappers.add(dsw1);
        dataSourceWrappers.add(dsw2);
        tgds.init(dataSourceWrappers);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        tgds = null;
    }

    @Before
    public void setUp() {
        MockDataSource.clearTrace();
    }

    @Test
    public void java_sql_Statement_api_support() throws Exception {
        TGroupConnection conn = null;
        Statement stmt = null;
        try {
            conn = tgds.getConnection();
            stmt = conn.createStatement();

            String insertSQL = "insert into crud(f1,f2) values(10,'str')";
            String updateSQL = "update crud set f2='str2'";
            String selectSQL = "select * from crud";
            String showSQL = "show create table crud";

            assertFalse(stmt.execute(insertSQL));
            assertTrue(stmt.execute(selectSQL));
            assertTrue(stmt.execute(showSQL));
            assertFalse(stmt.execute(insertSQL, Statement.RETURN_GENERATED_KEYS));
            assertFalse(stmt.execute(insertSQL, new int[] { 1 }));
            assertFalse(stmt.execute(insertSQL, new String[] { "col" }));

            stmt.addBatch(insertSQL);
            stmt.addBatch(updateSQL);

            int[] updateCounts = stmt.executeBatch();
            assertEquals(updateCounts.length, 2);
            MockDataSource.addPreData("id:1,name:2");
            assertTrue(stmt.executeQuery(selectSQL).next());
            assertEquals(stmt.executeUpdate(insertSQL), 1);
            assertEquals(stmt.executeUpdate(insertSQL, Statement.RETURN_GENERATED_KEYS), 1);
            assertEquals(stmt.executeUpdate(insertSQL, new int[] { 1 }), 1);
            assertEquals(stmt.executeUpdate(insertSQL, new String[] { "col" }), 1);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException e) {
                    }
                }
            }
        }
    }

    @Test
    public void testAddBatch() throws SQLException {
        TGroupConnection conn = null;
        PreparedStatement stat = null;
        try {
            conn = tgds.getConnection();
            stat = conn.prepareStatement("update test set type=? where id = ?");

            stat.setInt(1, 1);
            stat.setString(2, "2askjfoue33");
            stat.addBatch();

            stat.setInt(1, 2);
            stat.setString(2, "retrtorut48");
            stat.addBatch();

            int[] affectedRow = stat.executeBatch();
            System.out.println(Arrays.toString(affectedRow));
            MockDataSource.showTrace();
            Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "executeBatch"));
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
    public void testAddBatchSql() throws SQLException {
        TGroupConnection conn = null;
        Statement stat = null;
        try {
            conn = tgds.getConnection();
            stat = conn.createStatement();
            stat.addBatch("update t set name = 'newName' ");
            stat.addBatch("update t set type = 2 ");
            int[] affectedRow = stat.executeBatch();
            System.out.println(Arrays.toString(affectedRow));
            MockDataSource.showTrace();
            Assert.assertTrue(MockDataSource.hasMethod("db", "db1", "executeBatch"));
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

    /**
     * 同一个Statement先更新后查询
     */
    @Test
    public void testExecuteSql() throws SQLException {
        TGroupConnection conn = null;
        Statement stat = null;
        try {
            conn = tgds.getConnection();
            stat = conn.createStatement();
            boolean res = stat.execute("update t set name = 'newName'");
            Assert.assertEquals(res, false);
            res = stat.execute("select * from xxx where id=0");
            Assert.assertEquals(res, true);
            MockDataSource.showTrace();
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

    /**
     * 同一个PreparedStatement先更新后查询
     */
    @Test
    public void testExecute1() throws SQLException {
        TGroupConnection conn = null;
        PreparedStatement stat = null;
        try {
            conn = tgds.getConnection();
            stat = conn.prepareStatement("update t set name = 'newName' where date = ?");
            stat.setDate(1, new java.sql.Date(System.currentTimeMillis()));
            boolean res = stat.execute();
            Assert.assertEquals(res, false);
            res = stat.execute("select * from xxx where id=0");
            Assert.assertEquals(res, true);
            MockDataSource.showTrace();
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

    /**
     * 同一个PreparedStatement先查询后更新
     */
    @Test
    public void testExecute2() throws SQLException {
        TGroupConnection conn = null;
        PreparedStatement stat = null;
        try {
            conn = tgds.getConnection();
            stat = conn.prepareStatement("select * from xxx where id=?");
            stat.setByte(1, (byte) 5);
            boolean res = stat.execute();
            Assert.assertEquals(res, true);

            res = stat.execute("update t set name = 'newName'");
            Assert.assertEquals(res, false);
            MockDataSource.showTrace();
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
    public void testExecuteQuery() throws SQLException {
        TGroupConnection conn = null;
        PreparedStatement stat = null;
        try {
            conn = tgds.getConnection();
            stat = conn.prepareStatement("select * from xxx where id=?");
            stat.setByte(1, (byte) 5);
            MockDataSource.addPreData("id:1,name:2");
            ResultSet result = stat.executeQuery();
            Assert.assertEquals(result.next(), true);
            Assert.assertEquals(result.getLong(1), 1L);
            Assert.assertEquals(result.getLong(2), 2L);
            MockDataSource.showTrace();
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
    public void testExecuteUpdate() throws SQLException {
        TGroupConnection conn = null;
        PreparedStatement stat = null;
        try {
            conn = tgds.getConnection();
            stat = conn.prepareStatement("update xxxx set name = 'newname'");
            int affect = stat.executeUpdate();
            Assert.assertEquals(affect, 1);
            MockDataSource.showTrace();
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
