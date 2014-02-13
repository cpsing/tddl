package com.taobao.tddl.matrix.test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.matrix.jdbc.TConnection;
import com.taobao.tddl.matrix.jdbc.TDataSource;
import com.taobao.tddl.repo.mysql.spi.My_Transaction;

public class TransactionTest {

    static TDataSource ds = new TDataSource();

    @BeforeClass
    public static void initTestWithDS() throws TddlException, SQLException {
        ds.setAppName("andor_show");
        ds.setTopologyFile("test_matrix.xml");
        ds.setSchemaFile("test_schema.xml");

        ds.init();
    }

    @Test
    public void testNotAutoCommit() throws Exception {
        TConnection conn = (TConnection) ds.getConnection();
        conn.setAutoCommit(false);
        ExecutionContext context = conn.getExecutionContext();
        Assert.assertTrue(context.getTransaction() == null);
        {
            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
            ResultSet rs = ps.executeQuery();
            rs.next();
            rs.close();
        }

        Assert.assertTrue(context.getTransaction() != null);
        My_Transaction t = (My_Transaction) context.getTransaction();

        Assert.assertEquals("My_Transaction", t.getClass().getSimpleName());
        Assert.assertEquals("andor_show_group1", context.getTransactionGroup());

        Assert.assertEquals(1, t.getConnMap().size());

        {
            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
            ResultSet rs = ps.executeQuery();
            rs.next();
            rs.close();
        }

        Assert.assertEquals(t, conn.getExecutionContext().getTransaction());
        Assert.assertEquals(1, t.getConnMap().size());
        conn.commit();
        conn.close();

        Assert.assertEquals(0, t.getConnMap().size());

    }

    @Test
    public void testNotAutoCommitOnDifferentGroup() throws Exception {
        TConnection conn = (TConnection) ds.getConnection();
        conn.setAutoCommit(false);
        ExecutionContext context = conn.getExecutionContext();
        Assert.assertTrue(context.getTransaction() == null);
        {
            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
            ResultSet rs = ps.executeQuery();
            rs.next();
            rs.close();
        }

        Assert.assertTrue(context.getTransaction() != null);
        My_Transaction t = (My_Transaction) context.getTransaction();
        Assert.assertEquals("My_Transaction", t.getClass().getSimpleName());
        Assert.assertEquals("andor_show_group1", context.getTransactionGroup());
        Assert.assertEquals(1, t.getConnMap().size());
        {
            try {
                PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=2");
                ResultSet rs = ps.executeQuery();
                rs.next();
                rs.close();
                Assert.fail();
            } catch (Exception ex) {
                Assert.assertTrue(ex.getMessage().contains("transaction across group is not supported"));
            }
        }

        Assert.assertEquals(context, conn.getExecutionContext());
        Assert.assertEquals(t, conn.getExecutionContext().getTransaction());
        Assert.assertEquals(1, t.getConnMap().size());
        conn.commit();
        conn.close();

        Assert.assertEquals(0, t.getConnMap().size());

    }

    @Test
    public void testAutoCommit() throws Exception {
        TConnection conn = (TConnection) ds.getConnection();
        conn.setAutoCommit(true);

        ExecutionContext context = conn.getExecutionContext();
        Assert.assertTrue(context.getTransaction() == null);
        My_Transaction t1 = null;
        {
            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
            ResultSet rs = ps.executeQuery();
            rs.next();

            context = conn.getExecutionContext();
            Assert.assertTrue(context.getTransaction() != null);
            t1 = (My_Transaction) context.getTransaction();
            Assert.assertEquals("My_Transaction", t1.getClass().getSimpleName());
            Assert.assertEquals("andor_show_group1", context.getTransactionGroup());
            // autocommit, 事务链接不共享，单独管理
            Assert.assertEquals(0, t1.getConnMap().size());
            rs.close();
            Assert.assertEquals(0, t1.getConnMap().size());
        }

        My_Transaction t2 = null;
        {
            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
            ResultSet rs = ps.executeQuery();
            context = conn.getExecutionContext();
            rs.next();

            t2 = (My_Transaction) context.getTransaction();
            Assert.assertEquals(0, t2.getConnMap().size());
            rs.close();
            Assert.assertEquals(0, t2.getConnMap().size());
        }

        Assert.assertTrue(t1 != t2);
        conn.commit();
        conn.close();

        Assert.assertEquals(0, t1.getConnMap().size());
        Assert.assertEquals(0, t2.getConnMap().size());
    }

    /**
     * 先做非auto的，再做auto的 两次的连接都应该被关闭
     * 
     * @throws Exception
     */
    @Test
    public void testNotAutoCommit2() throws Exception {
        TConnection conn = (TConnection) ds.getConnection();
        conn.setAutoCommit(false);
        ExecutionContext context1 = conn.getExecutionContext();
        Assert.assertTrue(context1.getTransaction() == null);
        {
            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
            ResultSet rs = ps.executeQuery();

            Assert.assertEquals(context1, conn.getExecutionContext());
            rs.next();
            rs.close();
        }

        Assert.assertTrue(context1.getTransaction() != null);
        My_Transaction t1 = (My_Transaction) context1.getTransaction();

        Assert.assertEquals("My_Transaction", t1.getClass().getSimpleName());
        Assert.assertEquals("andor_show_group1", context1.getTransactionGroup());
        Assert.assertEquals(1, t1.getConnMap().size());
        {
            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
            ResultSet rs = ps.executeQuery();

            Assert.assertEquals(context1, conn.getExecutionContext());
            rs.next();
            rs.close();
        }

        Assert.assertEquals(t1, conn.getExecutionContext().getTransaction());
        Assert.assertEquals(1, t1.getConnMap().size());
        conn.commit();
        // commit之后，事务就会关闭
        Assert.assertEquals(0, t1.getConnMap().size());
        conn.setAutoCommit(true);
        {
            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=2");
            ResultSet rs = ps.executeQuery();
            rs.next();
            rs.close();
        }

        ExecutionContext context2 = conn.getExecutionContext();
        My_Transaction t2 = (My_Transaction) context2.getTransaction();
        Assert.assertEquals("andor_show_group0", context2.getTransactionGroup());
        Assert.assertTrue(context2 != context1);
        conn.close();
        Assert.assertEquals(0, t1.getConnMap().size());
        Assert.assertEquals(0, t2.getConnMap().size());
    }

}
