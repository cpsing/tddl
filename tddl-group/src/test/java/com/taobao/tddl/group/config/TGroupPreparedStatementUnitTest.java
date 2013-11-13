package com.taobao.tddl.group.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.Test;

import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.group.jdbc.DataSourceWrapper;
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import com.taobao.tddl.group.testutil.DBHelper;
import com.taobao.tddl.group.testutil.DataSourceFactory;

/**
 * @author yangzhu
 */
public class TGroupPreparedStatementUnitTest {

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        DBHelper.deleteAll();
    }

    @Test
    public void java_sql_Statement_api_support() throws Exception {
        TGroupDataSource ds = new TGroupDataSource();
        ds.init(new DataSourceWrapper("dbKey1", "rw", DataSourceFactory.getMySQLDataSource(), DBType.MYSQL));

        String insertSQL = "insert into crud(f1,f2) values(10,'str')";
        String updateSQL = "update crud set f2='str2'";
        String selectSQL = "select * from crud";

        Connection conn = ds.getConnection();
        PreparedStatement stmt = conn.prepareStatement(insertSQL);

        assertFalse(stmt.execute());
        stmt.close();

        stmt = conn.prepareStatement(selectSQL);
        assertTrue(stmt.execute());
        stmt.close();

        stmt = conn.prepareStatement(selectSQL);
        assertTrue(stmt.executeQuery().next());
        stmt.close();

        stmt = conn.prepareStatement(updateSQL);
        assertFalse(stmt.execute());
        stmt.close();

        stmt = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
        assertFalse(stmt.execute());
        stmt.close();

        stmt = conn.prepareStatement(insertSQL, new int[] { 1 });
        assertFalse(stmt.execute());
        stmt.close();

        stmt = conn.prepareStatement(insertSQL, new String[] { "col" });
        assertFalse(stmt.execute());

        assertEquals(stmt.executeUpdate(), 1);
        stmt.close();

        // ������������
        stmt = conn.prepareStatement("insert into crud(f1,f2) values(?,?)");
        stmt.setInt(1, 10);
        stmt.setString(2, "str1");
        stmt.addBatch();

        stmt.setInt(1, 20);
        stmt.setString(2, "str2");
        stmt.addBatch();

        int[] updateCounts = stmt.executeBatch();
        assertEquals(updateCounts.length, 2);
        stmt.close();

        conn.close();
    }
}
