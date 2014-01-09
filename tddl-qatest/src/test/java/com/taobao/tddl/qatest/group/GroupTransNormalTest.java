package com.taobao.tddl.qatest.group;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.tddl.common.GroupDataSourceRouteHelper;
import com.taobao.tddl.qatest.util.DateUtil;

/**
 * Comment for GroupTransNormalTest
 * <p/>
 * Created Date: 2010-12-10 下午07:48:14
 */
public class GroupTransNormalTest extends GroupTestCase {

    @Test
    public void oneConnCommitTest() throws SQLException {
        Connection conn = tds.getConnection();
        conn.setAutoCommit(false);
        Statement stat = conn.createStatement();
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (" + RANDOM_ID + ",'" + time + "')";
        stat.executeUpdate(sql);

        // 没提交之前使用同一个连接肯定查得到
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        String sqlx = "select * from normaltbl_0001 where pk=" + RANDOM_ID;
        ResultSet rs = stat.executeQuery(sqlx);
        while (rs.next()) {
            Assert.assertEquals(time, DateUtil.formatDate(rs.getDate("gmt_create"), DateUtil.DATE_FULLHYPHEN));
        }

        // 提交
        conn.commit();

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        ResultSet rs2 = stat.executeQuery(sqlx);
        while (rs2.next()) {
            // 肯定相同
            Assert.assertEquals(time, DateUtil.formatDate(rs2.getDate("gmt_create"), DateUtil.DATE_FULLHYPHEN));
        }

        // 多次提交或者回滚
        try {
            conn.commit();
            conn.commit();
            conn.rollback();
            conn.rollback();
        } catch (Exception e) {
            Assert.fail(e.toString());
        }
    }

    @Ignore("事务被锁跑的时间太久，暂时屏蔽")
    @Test
    public void severalConnCommitTest() throws SQLException {
        Connection conn = tds.getConnection();
        conn.setAutoCommit(false);
        Statement stat = conn.createStatement();
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (" + RANDOM_ID + ",'" + time + "')";
        stat.executeUpdate(sql);

        // 未提交，使用同一个连接肯定查得到
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        sql = "select * from normaltbl_0001 where pk=" + RANDOM_ID;
        ResultSet rs = stat.executeQuery(sql);
        while (rs.next()) {
            Assert.assertEquals(time, DateUtil.formatDate(rs.getDate("gmt_create"), DateUtil.DATE_FULLHYPHEN));
        }

        // 未提交，使用不同连接失败
        Connection otherConn = tds.getConnection();
        otherConn.setAutoCommit(false);
        Statement otherStat = otherConn.createStatement();
        sql = "insert into normaltbl_0001 (pk,gmt_create) values (" + RANDOM_ID + ",'" + time + "')";
        try {
            otherStat.executeUpdate(sql);
            Assert.fail();
        } catch (Exception e) {
            // Lock wait timeout exceeded; try restarting transaction
        }

        // 提交
        conn.commit();

        sql = "select * from normaltbl_0001 where pk=" + RANDOM_ID;
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        rs = stat.executeQuery(sql);
        while (rs.next()) {
            // 肯定相同
            Assert.assertEquals(time, DateUtil.formatDate(rs.getDate("gmt_create"), DateUtil.DATE_FULLHYPHEN));
        }
    }

    @Test
    public void oneConnRollbackTest() throws SQLException {
        Connection conn = tds.getConnection();
        conn.setAutoCommit(false);
        Statement stat = conn.createStatement();

        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (" + RANDOM_ID + ",'" + time + "')";
        stat.executeUpdate(sql);

        // 没提交之前使用同一个连接肯定查得到
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        String sqlx = "select * from normaltbl_0001 where pk=" + RANDOM_ID;
        ResultSet rs = stat.executeQuery(sqlx);
        while (rs.next()) {
            Assert.assertEquals(time, DateUtil.formatDate(rs.getDate("gmt_create"), DateUtil.DATE_FULLHYPHEN));
        }

        // 回滚
        conn.rollback();

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        rs = stat.executeQuery(sqlx);
        while (rs.next()) {
            // 肯定不相等
            Assert.assertNotSame(time, DateUtil.formatDate(rs.getDate("gmt_create"), DateUtil.DATE_FULLHYPHEN));
        }

        // 多次提交或者回滚
        try {
            conn.commit();
            conn.commit();
            conn.rollback();
            conn.rollback();
        } catch (Exception e) {
            Assert.fail(e.toString());
        }
    }

    @Ignore("事务被锁跑的时间太久，暂时屏蔽")
    @Test
    public void severalConnRollbackTest() throws SQLException {
        Connection conn = tds.getConnection();
        conn.setAutoCommit(false);
        Statement stat = conn.createStatement();

        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (" + RANDOM_ID + ",'" + time + "')";
        stat.executeUpdate(sql);

        // 未提交，使用同一个连接肯定查得到
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        String sqlx = "select * from normaltbl_0001 where pk=" + RANDOM_ID;
        ResultSet rs = stat.executeQuery(sqlx);
        while (rs.next()) {
            Assert.assertEquals(time, DateUtil.formatDate(rs.getDate("gmt_create"), DateUtil.DATE_FULLHYPHEN));
        }

        // 未提交，使用不同连接失败
        Connection otherConn = tds.getConnection();
        otherConn.setAutoCommit(false);
        Statement otherStat = otherConn.createStatement();
        sql = "insert into normaltbl_0001 (pk,gmt_create) values (" + RANDOM_ID + ",'" + time + "')";
        try {
            otherStat.executeUpdate(sql);
            Assert.fail();
        } catch (Exception e) {
            // Lock wait timeout exceeded; try restarting transaction
        }

        // 回滚
        conn.rollback();

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        rs = stat.executeQuery(sqlx);
        while (rs.next()) {
            // 肯定不相等
            Assert.assertNotSame(time, DateUtil.formatDate(rs.getDate("gmt_create"), DateUtil.DATE_FULLHYPHEN));
        }

        // 多次提交或者回滚
        try {
            conn.commit();
            conn.commit();
            conn.rollback();
            conn.rollback();
        } catch (Exception e) {
            Assert.fail(e.toString());
        }
    }
}
