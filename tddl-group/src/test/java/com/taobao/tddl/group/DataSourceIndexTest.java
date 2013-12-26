package com.taobao.tddl.group;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.common.GroupDataSourceRouteHelper;
import com.taobao.tddl.common.model.ThreadLocalString;
import com.taobao.tddl.common.utils.thread.ThreadLocalMap;
import com.taobao.tddl.group.jdbc.TGroupDataSource;

public class DataSourceIndexTest extends BaseGroupTest {

    @Test
    public void testThreadLocalDataSourceIndex() throws Exception {
        try {
            TGroupDataSource ds = new TGroupDataSource(GROUP0, APPNAME);
            MockServer.setConfigInfo(ds.getFullDbGroupKey(), DSKEY0 + ":rw" + "," + DSKEY1 + ":r");
            ds.init();

            ThreadLocalMap.put(ThreadLocalString.DATASOURCE_INDEX, 0);
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            assertEquals(stmt.executeUpdate("insert into tddl_test_0000(id,name) values(100,'str')"), 1);
            ResultSet rs = stmt.executeQuery("select id,name from tddl_test_0000 where id=100");
            assertTrue(rs.next());
            // 如果指定了index，忽略rw限制
            ThreadLocalMap.put(ThreadLocalString.DATASOURCE_INDEX, 1);
            assertEquals(stmt.executeUpdate("insert into tddl_test_0000(id,name) values(100,'str')"), 1);
            rs = stmt.executeQuery("select count(*) from tddl_test_0000 where id=100");
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);

            stmt.close();
            conn.close();
        } finally {
            ThreadLocalMap.put(ThreadLocalString.DATASOURCE_INDEX, null);
        }
    }

    @Test
    public void testGroupDataSourceRouteHelper() throws Exception {
        try {
            TGroupDataSource ds = new TGroupDataSource(GROUP0, APPNAME);
            MockServer.setConfigInfo(ds.getFullDbGroupKey(), DSKEY0 + ":rw" + "," + DSKEY1 + ":r");
            ds.init();

            GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            assertEquals(stmt.executeUpdate("insert into tddl_test_0000(id,name) values(100,'str')"), 1);
            ResultSet rs = stmt.executeQuery("select id,name from tddl_test_0000 where id=100");
            assertTrue(rs.next());
            // 如果指定了index，忽略rw限制
            ThreadLocalMap.put(ThreadLocalString.DATASOURCE_INDEX, 1);
            assertEquals(stmt.executeUpdate("insert into tddl_test_0000(id,name) values(100,'str')"), 1);
            rs = stmt.executeQuery("select count(*) from tddl_test_0000 where id=100");
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);

            stmt.close();
            conn.close();
        } finally {
            ThreadLocalMap.put(ThreadLocalString.DATASOURCE_INDEX, null);
        }
    }
}
