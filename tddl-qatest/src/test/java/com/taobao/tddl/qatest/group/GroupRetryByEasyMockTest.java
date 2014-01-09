package com.taobao.tddl.qatest.group;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import com.mysql.jdbc.CommunicationsException;

/**
 * Comment for GroupRetryByEasyMockTest
 * <p/>
 * Created Date: 2010-12-8 下午03:19:00
 */
public class GroupRetryByEasyMockTest extends GroupEasyMock {

    private String       sql = "select * from normaltbl_0000 where pk in (0)";
    private SQLException ex  = new CommunicationsException(null, 0, 0, null);

    @Test
    public void querySimpleRSAndExceptionOnSetAutoCommitTest() throws SQLException {
        // 跳过easyMock测试
        if (EASYMOCK_SHOULD_NOT_BE_TEST) {
            return;
        }

        for (int i = 0; i < 10; i++) {
            // -----------expect
            EasyMock.expect(ds.getConnection()).andReturn(conn).times(0, 1);
            EasyMock.expect(conn.getHoldability()).andReturn(-1).anyTimes();
            conn.setAutoCommit(true);
            EasyMock.expectLastCall().andThrow(ex).times(0, 1);
            conn.close();
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(conn.isClosed()).andReturn(false).times(0, 1);

            EasyMock.expect(ds1.getConnection()).andReturn(conn1).times(1);
            EasyMock.expect(conn1.getHoldability()).andReturn(-1).anyTimes();
            EasyMock.expect(conn1.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
                .andReturn(ps1)
                .times(0, 1);
            conn1.setAutoCommit(true);
            EasyMock.expectLastCall().times(1);
            ps1.setQueryTimeout(0);
            EasyMock.expectLastCall().times(1);
            ps1.setFetchSize(0);
            EasyMock.expectLastCall().times(0, 1);
            ps1.setMaxRows(0);
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(ps1.executeQuery()).andReturn(rs1).times(1);
            EasyMock.expect(rs1.next()).andReturn(true).times(1);
            EasyMock.expect(rs1.getObject(1)).andReturn(1).times(1);
            rs1.close();
            EasyMock.expectLastCall().times(2);
            ps1.close();
            EasyMock.expectLastCall().times(1);
            conn1.close();
            EasyMock.expectLastCall().times(1);
            EasyMock.expect(conn1.isClosed()).andReturn(false).times(0, 1);

            // -----------test
            ctl.replay();
            Connection tconn = tds.getConnection();
            PreparedStatement tps = tconn.prepareStatement(sql);
            Assert.assertTrue(tps.execute());
            ResultSet trs = tps.getResultSet();
            Assert.assertTrue(trs.next());
            Assert.assertEquals(1, trs.getObject(1));
            trs.close();
            tps.close();
            tconn.close();
            ctl.verify();
            ctl.reset();
        }
    }

    @Test
    public void querySimpleRSAndExceptionOnQueryTest() throws SQLException {
        // 跳过easyMock测试
        if (EASYMOCK_SHOULD_NOT_BE_TEST) {
            return;
        }

        for (int i = 0; i < 10; i++) {
            // -----------expect
            EasyMock.expect(ds.getConnection()).andReturn(conn).times(0, 1);
            EasyMock.expect(conn.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
                .andThrow(ex)
                .times(0, 1);
            EasyMock.expect(conn.getHoldability()).andReturn(-1).anyTimes();
            conn.setAutoCommit(true);
            EasyMock.expectLastCall().times(0, 1);
            conn.close();
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(conn.isClosed()).andReturn(false).times(0, 1);

            EasyMock.expect(ds1.getConnection()).andReturn(conn1).times(1);
            EasyMock.expect(conn1.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
                .andReturn(ps1)
                .times(0, 1);
            EasyMock.expect(conn1.getHoldability()).andReturn(-1).anyTimes();
            conn1.setAutoCommit(true);
            EasyMock.expectLastCall().times(0, 1);
            ps1.setQueryTimeout(0);
            EasyMock.expectLastCall().times(1);
            ps1.setFetchSize(0);
            EasyMock.expectLastCall().times(0, 1);
            ps1.setMaxRows(0);
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(ps1.executeQuery()).andReturn(rs1).times(1);
            EasyMock.expect(rs1.next()).andReturn(true).times(1);
            EasyMock.expect(rs1.getObject(1)).andReturn(1).times(1);
            rs1.close();
            EasyMock.expectLastCall().times(2);
            ps1.close();
            EasyMock.expectLastCall().times(1);
            conn1.close();
            EasyMock.expectLastCall().times(1);
            EasyMock.expect(conn1.isClosed()).andReturn(false).times(0, 1);

            // -----------test
            ctl.replay();
            Connection tconn = tds.getConnection();
            PreparedStatement tps = tconn.prepareStatement(sql);
            Assert.assertTrue(tps.execute());
            ResultSet trs = tps.getResultSet();
            Assert.assertTrue(trs.next());
            Assert.assertEquals(1, trs.getObject(1));
            trs.close();
            tps.close();
            tconn.close();

            ctl.verify();
            ctl.reset();
        }
    }

    @Test
    public void querySimpleRSAndExceptionOnSetQueryTimeOutTest() throws SQLException {
        // 跳过easyMock测试
        if (EASYMOCK_SHOULD_NOT_BE_TEST) {
            return;
        }

        for (int i = 0; i < 10; i++) {
            // -----------expect
            EasyMock.expect(ds.getConnection()).andReturn(conn).times(0, 1);
            EasyMock.expect(conn.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
                .andReturn(ps)
                .times(0, 1);
            conn.setAutoCommit(true);
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(conn.getHoldability()).andReturn(-1).anyTimes();
            ps.setQueryTimeout(0);
            EasyMock.expectLastCall().andThrow(ex).times(0, 1);
            ps.close();
            EasyMock.expectLastCall().times(0, 1);
            conn.close();
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(conn.isClosed()).andReturn(false).times(0, 1);

            EasyMock.expect(ds1.getConnection()).andReturn(conn1).times(1);
            EasyMock.expect(conn1.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
                .andReturn(ps1)
                .times(1);
            conn1.setAutoCommit(true);
            EasyMock.expectLastCall().times(1);
            EasyMock.expect(conn1.getHoldability()).andReturn(-1).anyTimes();
            ps1.setQueryTimeout(0);
            EasyMock.expectLastCall().times(1);
            ps1.setFetchSize(0);
            EasyMock.expectLastCall().times(0, 1);
            ps1.setMaxRows(0);
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(ps1.executeQuery()).andReturn(rs1).times(1);
            EasyMock.expect(rs1.next()).andReturn(true).times(1);
            EasyMock.expect(rs1.getObject(1)).andReturn(1).times(1);
            rs1.close(); // 外部关了一次内部又关了一次
            EasyMock.expectLastCall().times(2);
            ps1.close();
            EasyMock.expectLastCall().times(1);
            conn1.close();
            EasyMock.expectLastCall().times(1);
            EasyMock.expect(conn1.isClosed()).andReturn(false).times(0, 1);

            // -----------test
            ctl.replay();
            Connection tconn = tds.getConnection();
            PreparedStatement tps = tconn.prepareStatement(sql);
            Assert.assertTrue(tps.execute());
            ResultSet trs = tps.getResultSet();
            Assert.assertTrue(trs.next());
            Assert.assertEquals(1, trs.getObject(1));
            trs.close();
            tps.close();
            tconn.close();

            ctl.verify();
            ctl.reset();
        }
    }

    @Test
    public void querySimpleRSAndExceptionOnSetObjectTest() throws SQLException {
        // 跳过easyMock测试
        if (EASYMOCK_SHOULD_NOT_BE_TEST) {
            return;
        }

        for (int i = 0; i < 10; i++) {
            // -----------expect
            EasyMock.expect(ds.getConnection()).andReturn(conn).times(0, 1);
            EasyMock.expect(conn.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
                .andReturn(ps)
                .times(0, 1);
            EasyMock.expect(conn.getHoldability()).andReturn(-1).anyTimes();
            conn.setAutoCommit(true);
            EasyMock.expectLastCall().times(0, 1);
            ps.setObject(1, 1);
            EasyMock.expectLastCall().andThrow(ex).times(0, 1);
            ps.setQueryTimeout(0);
            EasyMock.expectLastCall().times(0, 1);
            ps.setFetchSize(0);
            EasyMock.expectLastCall().times(0, 1);
            ps.setMaxRows(0);
            EasyMock.expectLastCall().times(0, 1);
            ps.close();
            EasyMock.expectLastCall().times(0, 1);
            conn.close();
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(conn.isClosed()).andReturn(false).times(0, 1);

            EasyMock.expect(ds1.getConnection()).andReturn(conn1).times(1);
            EasyMock.expect(conn1.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
                .andReturn(ps1)
                .times(0, 1);
            EasyMock.expect(conn1.getHoldability()).andReturn(-1).anyTimes();
            conn1.setAutoCommit(true);
            EasyMock.expectLastCall().times(1);
            ps1.setObject(1, 1);
            EasyMock.expectLastCall().times(1);
            ps1.setQueryTimeout(0);
            EasyMock.expectLastCall().times(1);
            ps1.setFetchSize(0);
            EasyMock.expectLastCall().times(0, 1);
            ps1.setMaxRows(0);
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(ps1.executeQuery()).andReturn(rs1).times(1);
            EasyMock.expect(rs1.next()).andReturn(true).times(1);
            EasyMock.expect(rs1.getObject(1)).andReturn(1).times(1);
            rs1.close();
            EasyMock.expectLastCall().times(2);
            ps1.close();
            EasyMock.expectLastCall().times(1);
            conn1.close();
            EasyMock.expectLastCall().times(1);
            EasyMock.expect(conn1.isClosed()).andReturn(false).times(0, 1);

            // -----------test
            ctl.replay();
            Connection tconn = tds.getConnection();
            PreparedStatement tps = tconn.prepareStatement(sql);
            tps.setObject(1, 1);
            Assert.assertTrue(tps.execute());
            ResultSet trs = tps.getResultSet();
            Assert.assertTrue(trs.next());
            Assert.assertEquals(1, trs.getObject(1));
            trs.close();
            tps.close();
            tconn.close();

            ctl.verify();
            ctl.reset();
        }
    }

    @Test
    public void querySimpleRSAndExceptionOnExeQueryTest() throws SQLException {
        // 跳过easyMock测试
        if (EASYMOCK_SHOULD_NOT_BE_TEST) {
            return;
        }

        for (int i = 0; i < 10; i++) {
            // -----------expect
            EasyMock.expect(ds.getConnection()).andReturn(conn).times(0, 1);
            EasyMock.expect(conn.getHoldability()).andReturn(-1).anyTimes();
            EasyMock.expect(conn.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
                .andReturn(ps)
                .times(0, 1);
            conn.setAutoCommit(true);
            EasyMock.expectLastCall().times(0, 1);
            ps.setObject(1, 1);
            EasyMock.expectLastCall().times(0, 1);
            ps.setQueryTimeout(0);
            EasyMock.expectLastCall().times(0, 1);
            ps.setFetchSize(0);
            EasyMock.expectLastCall().times(0, 1);
            ps.setMaxRows(0);
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(ps.executeQuery()).andThrow(ex).times(0, 1);
            ps.close();
            EasyMock.expectLastCall().times(0, 1);
            conn.close();
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(conn.isClosed()).andReturn(false).times(0, 1);

            EasyMock.expect(ds1.getConnection()).andReturn(conn1).times(1);
            EasyMock.expect(conn1.getHoldability()).andReturn(-1).anyTimes();
            EasyMock.expect(conn1.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
                .andReturn(ps1)
                .times(0, 1);
            conn1.setAutoCommit(true);
            EasyMock.expectLastCall().times(1);
            ps1.setObject(1, 1);
            EasyMock.expectLastCall().times(1);
            ps1.setQueryTimeout(0);
            EasyMock.expectLastCall().times(1);
            ps1.setFetchSize(0);
            EasyMock.expectLastCall().times(0, 1);
            ps1.setMaxRows(0);
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(ps1.executeQuery()).andReturn(rs1).times(1);
            EasyMock.expect(rs1.next()).andReturn(true).times(1);
            EasyMock.expect(rs1.getObject(1)).andReturn(1).times(1);
            rs1.close();
            EasyMock.expectLastCall().times(2);
            ps1.close();
            EasyMock.expectLastCall().times(1);
            conn1.close();
            EasyMock.expectLastCall().times(1);
            EasyMock.expect(conn1.isClosed()).andReturn(false).times(0, 1);

            // -----------test
            ctl.replay();
            Connection tconn = tds.getConnection();
            PreparedStatement tps = tconn.prepareStatement(sql);
            tps.setObject(1, 1);
            Assert.assertTrue(tps.execute());
            ResultSet trs = tps.getResultSet();
            Assert.assertTrue(trs.next());
            Assert.assertEquals(1, trs.getObject(1));
            trs.close();
            tps.close();
            tconn.close();

            ctl.verify();
            ctl.reset();
        }
    }

    @Test
    public void querySimpleRSAndExceptionOnGetObjectTest() throws SQLException {
        // 跳过easyMock测试
        if (EASYMOCK_SHOULD_NOT_BE_TEST) {
            return;
        }

        for (int i = 0; i < 10; i++) {
            // -----------expect
            EasyMock.expect(ds.getConnection()).andReturn(conn).times(0, 1);
            EasyMock.expect(conn.getHoldability()).andReturn(-1).anyTimes();
            EasyMock.expect(conn.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
                .andReturn(ps)
                .times(0, 1);
            conn.setAutoCommit(true);
            EasyMock.expectLastCall().times(0, 1);
            ps.setObject(1, 1);
            EasyMock.expectLastCall().times(0, 1);
            ps.setQueryTimeout(0);
            EasyMock.expectLastCall().times(0, 1);
            ps.setFetchSize(0);
            EasyMock.expectLastCall().times(0, 1);
            ps.setMaxRows(0);
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(ps.executeQuery()).andReturn(rs).times(0, 1);
            EasyMock.expect(rs.next()).andReturn(true).times(0, 1);
            EasyMock.expect(rs.getObject(1)).andThrow(ex).times(0, 1);
            rs.close();
            EasyMock.expectLastCall().times(0, 2);
            ps.close();
            EasyMock.expectLastCall().times(0, 1);
            conn.close();
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(conn.isClosed()).andReturn(false).times(0, 1);

            EasyMock.expect(ds1.getConnection()).andReturn(conn1).times(0, 1);
            EasyMock.expect(conn1.getHoldability()).andReturn(-1).anyTimes();
            EasyMock.expect(conn1.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
                .andReturn(ps1)
                .times(0, 1);
            conn1.setAutoCommit(true);
            EasyMock.expectLastCall().times(0, 1);
            ps1.setObject(1, 1);
            EasyMock.expectLastCall().times(0, 1);
            ps1.setQueryTimeout(0);
            EasyMock.expectLastCall().times(0, 1);
            ps1.setFetchSize(0);
            EasyMock.expectLastCall().times(0, 1);
            ps1.setMaxRows(0);
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(ps1.executeQuery()).andReturn(rs1).times(0, 1);
            EasyMock.expect(rs1.next()).andReturn(true).times(0, 1);
            EasyMock.expect(rs1.getObject(1)).andReturn(1).times(0, 1);
            rs1.close();
            EasyMock.expectLastCall().times(0, 2);
            ps1.close();
            EasyMock.expectLastCall().times(0, 1);
            conn1.close();
            EasyMock.expectLastCall().times(0, 1);
            EasyMock.expect(conn1.isClosed()).andReturn(false).times(0, 1);

            // -----------test
            ctl.replay();
            Connection tconn = tds.getConnection();
            PreparedStatement tps = tconn.prepareStatement(sql);
            tps.setObject(1, 1);
            Assert.assertTrue(tps.execute());
            ResultSet trs = tps.getResultSet();
            Assert.assertTrue(trs.next());
            try {
                Assert.assertEquals(1, trs.getObject(1));
            } catch (SQLException e) {
                Assert.assertEquals("Communications link failure", e.getMessage());
            }
            trs.close();
            tps.close();
            tconn.close();

            ctl.verify();
            ctl.reset();
        }
    }

    @Test
    public void querySimpleRSAndExceptionOnRsCloseTest() throws SQLException {
        // 跳过easyMock测试
        if (EASYMOCK_SHOULD_NOT_BE_TEST) {
            return;
        }

        // -----------expect
        EasyMock.expect(ds.getConnection()).andReturn(conn).times(0, 1);
        EasyMock.expect(conn.getHoldability()).andReturn(-1).anyTimes();
        EasyMock.expect(conn.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
            .andReturn(ps)
            .times(0, 1);
        conn.setAutoCommit(true);
        EasyMock.expectLastCall().times(0, 1);
        ps.setQueryTimeout(0);
        EasyMock.expectLastCall().times(0, 1);
        ps.setFetchSize(0);
        EasyMock.expectLastCall().times(0, 1);
        ps.setMaxRows(0);
        EasyMock.expectLastCall().times(0, 1);
        EasyMock.expect(ps.executeQuery()).andReturn(rs).times(0, 1);
        EasyMock.expect(rs.next()).andReturn(true).times(0, 1);
        EasyMock.expect(rs.getObject(1)).andReturn(1).times(0, 1);
        rs.close();
        EasyMock.expectLastCall().andThrow(ex).times(0, 2);
        ps.close();
        EasyMock.expectLastCall().times(0, 1);
        conn.close();
        EasyMock.expectLastCall().times(0, 1);
        EasyMock.expect(conn.isClosed()).andReturn(false).times(0, 1);

        EasyMock.expect(ds1.getConnection()).andReturn(conn1).times(0, 1);
        EasyMock.expect(conn1.getHoldability()).andReturn(-1).anyTimes();
        EasyMock.expect(conn1.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
            .andReturn(ps1)
            .times(0, 1);
        conn1.setAutoCommit(true);
        EasyMock.expectLastCall().times(0, 1);
        ps1.setQueryTimeout(0);
        EasyMock.expectLastCall().times(0, 1);
        ps1.setFetchSize(0);
        EasyMock.expectLastCall().times(0, 1);
        ps1.setMaxRows(0);
        EasyMock.expectLastCall().times(0, 1);
        EasyMock.expect(ps1.executeQuery()).andReturn(rs1).times(0, 1);
        EasyMock.expect(rs1.next()).andReturn(true).times(0, 1);
        EasyMock.expect(rs1.getObject(1)).andReturn(1).times(0, 1);
        rs1.close();
        EasyMock.expectLastCall().andThrow(ex).times(0, 2);
        ps1.close();
        EasyMock.expectLastCall().times(0, 1);
        conn1.close();
        EasyMock.expectLastCall().times(0, 1);
        EasyMock.expect(conn1.isClosed()).andReturn(false).times(0, 1);

        // -----------test
        ctl.replay();
        Connection tconn = tds.getConnection();
        PreparedStatement tps = tconn.prepareStatement(sql);
        Assert.assertTrue(tps.execute());
        ResultSet trs = tps.getResultSet();
        Assert.assertTrue(trs.next());
        Assert.assertEquals(1, trs.getObject(1));
        try {
            trs.close();
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals("Communications link failure", e.getMessage());
        }
        tps.close(); // ps内部在关闭rs的时候，将catchw住rs抛出的Communications link
                     // failure异常，并且打印出warn日志
        tconn.close();

        ctl.verify();
        ctl.reset();
    }

    @Test
    public void querySimpleRSAndExceptionOnPsCloseTest() throws SQLException {
        // 跳过easyMock测试
        if (EASYMOCK_SHOULD_NOT_BE_TEST) {
            return;
        }

        // -----------expect
        EasyMock.expect(ds.getConnection()).andReturn(conn).times(0, 1);
        EasyMock.expect(conn.getHoldability()).andReturn(-1).anyTimes();
        EasyMock.expect(conn.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
            .andReturn(ps)
            .times(0, 1);
        conn.setAutoCommit(true);
        EasyMock.expectLastCall().times(0, 1);
        ps.setQueryTimeout(0);
        EasyMock.expectLastCall().times(0, 1);
        ps.setFetchSize(0);
        EasyMock.expectLastCall().times(0, 1);
        ps.setMaxRows(0);
        EasyMock.expectLastCall().times(0, 1);
        EasyMock.expect(ps.executeQuery()).andReturn(rs).times(0, 1);
        EasyMock.expect(rs.next()).andReturn(true).times(0, 1);
        EasyMock.expect(rs.getObject(1)).andReturn(1).times(0, 1);
        rs.close();
        EasyMock.expectLastCall().times(0, 2);
        ps.close();
        EasyMock.expectLastCall().andThrow(ex).times(0, 1);
        conn.close();
        EasyMock.expectLastCall().times(0, 1);
        EasyMock.expect(conn.isClosed()).andReturn(false).times(0, 1);

        EasyMock.expect(ds1.getConnection()).andReturn(conn1).times(0, 1);
        EasyMock.expect(conn1.getHoldability()).andReturn(-1).anyTimes();
        EasyMock.expect(conn1.prepareStatement(sql, resultSetType, resultSetConcurrency, holdablity))
            .andReturn(ps1)
            .times(0, 1);
        conn1.setAutoCommit(true);
        EasyMock.expectLastCall().times(0, 1);
        ps1.setQueryTimeout(0);
        EasyMock.expectLastCall().times(0, 1);
        ps1.setFetchSize(0);
        EasyMock.expectLastCall().times(0, 1);
        ps1.setMaxRows(0);
        EasyMock.expectLastCall().times(0, 1);
        EasyMock.expect(ps1.executeQuery()).andReturn(rs1).times(0, 1);
        EasyMock.expect(rs1.next()).andReturn(true).times(0, 1);
        EasyMock.expect(rs1.getObject(1)).andReturn(1).times(0, 1);
        rs1.close();
        EasyMock.expectLastCall().times(0, 2);
        ps1.close();
        EasyMock.expectLastCall().andThrow(ex).times(0, 1);
        conn1.close();
        EasyMock.expectLastCall().times(0, 1);
        EasyMock.expect(conn1.isClosed()).andReturn(false).times(0, 1);

        // -----------test
        ctl.replay();
        Connection tconn = tds.getConnection();
        PreparedStatement tps = tconn.prepareStatement(sql);
        Assert.assertTrue(tps.execute());
        ResultSet trs = tps.getResultSet();
        Assert.assertTrue(trs.next());
        Assert.assertEquals(1, trs.getObject(1));
        trs.close();
        try {
            tps.close();
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals("Communications link failure", e.getMessage());
        }
        tconn.close();

        ctl.verify();
        ctl.reset();
    }
}
