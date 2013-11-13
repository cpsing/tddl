package com.taobao.tddl.group.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
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
public class TGroupStatementUnitTest {

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        DBHelper.deleteAll();
    }

    @Test
    public void java_sql_Statement_api_support() throws Exception {
        TGroupDataSource ds = new TGroupDataSource();
        ds.init(new DataSourceWrapper("dbKey1", "rw", DataSourceFactory.getMySQLDataSource(), DBType.MYSQL));

        Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();

        String insertSQL = "insert into crud(f1,f2) values(10,'str')";
        String updateSQL = "update crud set f2='str2'";
        String selectSQL = "select * from crud";
        String showSQL = "show create table crud";

        // Statement.execute����һ�����Ϊ ResultSet �����򷵻�
        // true�������Ϊ���¼�����߲������κν���򷵻� false
        assertFalse(stmt.execute(insertSQL));
        assertTrue(stmt.execute(selectSQL));
        assertTrue(stmt.execute(showSQL));

        assertFalse(stmt.execute(insertSQL, Statement.RETURN_GENERATED_KEYS));
        assertTrue(stmt.getGeneratedKeys().next());

        assertFalse(stmt.execute(insertSQL, new int[] { 1 }));
        assertTrue(stmt.getGeneratedKeys().next());

        assertFalse(stmt.execute(insertSQL, new String[] { "col" }));
        assertTrue(stmt.getGeneratedKeys().next());

        stmt.addBatch(insertSQL);
        stmt.addBatch(updateSQL);

        int[] updateCounts = stmt.executeBatch();

        assertEquals(updateCounts.length, 2);

        assertTrue(stmt.executeQuery(selectSQL).next());

        assertEquals(stmt.executeUpdate(insertSQL), 1);

        assertEquals(stmt.executeUpdate(insertSQL, Statement.RETURN_GENERATED_KEYS), 1);
        assertTrue(stmt.getGeneratedKeys().next());

        assertEquals(stmt.executeUpdate(insertSQL, new int[] { 1 }), 1);
        assertTrue(stmt.getGeneratedKeys().next());

        assertEquals(stmt.executeUpdate(insertSQL, new String[] { "col" }), 1);
        assertTrue(stmt.getGeneratedKeys().next());

        stmt.close();
        conn.close();
    }
}
