package com.taobao.tddl.group;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.junit.Test;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.group.jdbc.DataSourceWrapper;
import com.taobao.tddl.group.jdbc.TGroupDataSource;

public class CrudTest extends BaseGroupTest {

    @Test
    public void 单个数据库() throws Exception {
        TGroupDataSource ds = new TGroupDataSource();
        DataSourceWrapper dsw = new DataSourceWrapper(DSKEY0, "rw", getMySQLDataSource(), DBType.MYSQL);
        ds.init(dsw);

        testCrud(ds);
    }

    @Test
    public void 单个数据库_atom() throws Exception {
        TGroupDataSource ds = new TGroupDataSource();
        ds.setDbGroupKey(GROUP0);
        ds.setAppName(APPNAME);
        MockServer.setConfigInfo(ds.getFullDbGroupKey(), DSKEY1 + ":rw");
        ds.init();

        testCrud(ds);
    }

    @Test
    public void 测试DataSourceWrapper() throws Exception {
        List<DataSourceWrapper> dataSourceWrappers = new ArrayList<DataSourceWrapper>();
        dataSourceWrappers.add(new DataSourceWrapper(DSKEY1, "rw", getMySQLDataSource(1), DBType.MYSQL));
        dataSourceWrappers.add(new DataSourceWrapper(DSKEY2, "r", getMySQLDataSource(2), DBType.MYSQL));

        TGroupDataSource ds = new TGroupDataSource();
        ds.setDbGroupKey(GROUP0);
        ds.init(dataSourceWrappers);

        testCrud(ds);
    }

    @Test
    public void 测试DataSourceWrapper_atom() throws Exception {
        TGroupDataSource ds = new TGroupDataSource();
        ds.setDbGroupKey(GROUP0);
        ds.setAppName(APPNAME);
        MockServer.setConfigInfo(ds.getFullDbGroupKey(), DSKEY0 + ":rw" + "," + DSKEY1 + ":r");
        ds.init();

        testCrud(ds);
    }

    // dbGroup: db1:r10w, db2:r20, db3:r30
    @Test
    public void 三个数据库_测试db1可读写_db2与db3只能读() throws Exception {
        DataSource ds0 = getMySQLDataSource(0);
        DataSource ds1 = getMySQLDataSource(1);
        DataSource ds2 = getMySQLDataSource(2);

        // 读库时最有可能从db3读，然后是db2，db1的权重最小
        TGroupDataSource ds = new TGroupDataSource();
        DataSourceWrapper dsw0 = new DataSourceWrapper(DSKEY0, "r10w", ds0, DBType.MYSQL);
        DataSourceWrapper dsw1 = new DataSourceWrapper(DSKEY1, "r20", ds1, DBType.MYSQL);
        DataSourceWrapper dsw2 = new DataSourceWrapper(DSKEY2, "r30", ds2, DBType.MYSQL);
        ds.init(dsw0, dsw1, dsw2);

        testCrud(ds);
    }

    @Test
    public void 三个数据库_测试db1可读写_db2与db3只能读_atom() throws Exception {
        TGroupDataSource ds = new TGroupDataSource();
        ds.setDbGroupKey(GROUP0);
        ds.setAppName(APPNAME);
        MockServer.setConfigInfo(ds.getFullDbGroupKey(), DSKEY0 + ":r10w" + "," + DSKEY1 + ":r20" + "," + DSKEY2
                                                         + ":r30");
        ds.init();

        testCrud(ds);
    }

    private void testCrud(DataSource ds) throws SQLException {
        Connection conn = ds.getConnection();
        // 测试Statement的crud
        Statement stmt = conn.createStatement();
        assertEquals(stmt.executeUpdate("insert into tddl_test_0000(id,name,gmt_create,gmt_modified) values(10,'str',now(),now())"),
            1);
        assertEquals(stmt.executeUpdate("update tddl_test_0000 set name='str2'"), 1);
        ResultSet rs = stmt.executeQuery("select id,name from tddl_test_0000");
        assertEquals(true, rs.next());
        assertEquals(10, rs.getInt(1));
        assertEquals("str2", rs.getString(2));
        assertEquals(stmt.executeUpdate("delete from tddl_test_0000"), 1);
        rs.close();
        stmt.close();

        // 测试PreparedStatement的crud
        String sql = "insert into tddl_test_0000(id,name,gmt_create,gmt_modified) values(?,?,now(),now())";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, 10);
        ps.setString(2, "str");
        assertEquals(ps.executeUpdate(), 1);
        ps.close();

        sql = "update tddl_test_0000 set name=?";
        ps = conn.prepareStatement(sql);
        ps.setString(1, "str2");
        assertEquals(ps.executeUpdate(), 1);
        ps.close();

        sql = "select id,name from tddl_test_0000";
        ps = conn.prepareStatement(sql);
        rs = ps.executeQuery();
        rs.next();
        assertEquals(rs.getInt(1), 10);
        assertEquals(rs.getString(2), "str2");
        rs.close();
        ps.close();

        sql = "delete from tddl_test_0000";
        ps = conn.prepareStatement(sql);
        assertEquals(ps.executeUpdate(), 1);
        ps.close();
        conn.close();
    }

    // dbGroup: db1:w, db2:r20, db3:r30
    @Test
    public void 在只写库上更新后再查询会重用写库上的连接_即使它是一个只写库也不管() throws Exception { // 不支持这种只能写的情况
        DataSource ds0 = getMySQLDataSource(0);
        DataSource ds1 = getMySQLDataSource(1);
        DataSource ds2 = getMySQLDataSource(2);

        // 读库时最有可能从db3读，然后是db2，db1的权重最小
        TGroupDataSource ds = new TGroupDataSource();
        DataSourceWrapper dsw0 = new DataSourceWrapper(DSKEY0, "w", ds0, DBType.MYSQL);
        DataSourceWrapper dsw1 = new DataSourceWrapper(DSKEY1, "r20", ds1, DBType.MYSQL);
        DataSourceWrapper dsw2 = new DataSourceWrapper(DSKEY2, "r30", ds2, DBType.MYSQL);
        ds.init(dsw0, dsw1, dsw2);
        testCrud_Read(ds);
    }

    @Test
    public void 在只写库上更新后再查询会重用写库上的连接_即使它是一个只写库也不管_atom() throws Exception {
        TGroupDataSource ds = new TGroupDataSource();
        ds.setDbGroupKey(GROUP0);
        ds.setAppName(APPNAME);
        MockServer.setConfigInfo(ds.getFullDbGroupKey(), DSKEY0 + ":w" + "," + DSKEY1 + ":r20" + "," + DSKEY2 + ":r30");
        ds.init();

        testCrud_Read(ds);
    }

    private void testCrud_Read(TGroupDataSource ds) throws SQLException {
        Connection conn = ds.getConnection();

        Statement stmt = conn.createStatement();
        assertEquals(stmt.executeUpdate("insert into tddl_test_0000(id,name,gmt_create,gmt_modified) values(100,'str',now(),now())"),
            1);

        // 在只写库上更新后，会保留写连接，
        // 但是因为写连接对应的数据源被配置成只写，所以接下来的读操作不允许在写连接上进行
        // 因为db2,db3都没有数据，所以rs.next()返回false
        ResultSet rs = stmt.executeQuery("select id,name from tddl_test_0000 where id=100");
        assertFalse(rs.next());
        rs.close();

        assertEquals(stmt.executeUpdate("delete from tddl_test_0000 where id=100"), 1);
        stmt.close();
        conn.close();
    }
}
