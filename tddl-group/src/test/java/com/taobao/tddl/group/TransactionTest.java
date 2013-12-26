package com.taobao.tddl.group;

import static org.junit.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.group.jdbc.TGroupDataSource;

public class TransactionTest extends BaseGroupTest {

    @Test
    public void test单库事务() {
        TGroupDataSource ds = new TGroupDataSource(GROUP0, APPNAME);
        // 强制连dskey0库，读权重为20
        MockServer.setConfigInfo(ds.getFullDbGroupKey(), DSKEY0 + ":r20w" + "," + DSKEY1 + ":r");
        ds.init();

        // 构建spring 事务莫把呢
        final TransactionTemplate transactionTemplate = new TransactionTemplate();
        transactionTemplate.setTransactionManager(new DataSourceTransactionManager(ds));

        final JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);

        try {
            transactionTemplate.execute(new TransactionCallback() {

                public Object doInTransaction(TransactionStatus status) {
                    assertEquals(1,
                        jdbcTemplate.update("insert into tddl_test_0000(id,name,gmt_create,gmt_modified) values(10,'str',now(),now())"));
                    assertEquals(1, jdbcTemplate.update("update tddl_test_0000 set name='str2'"));
                    jdbcTemplate.execute(new StatementCallback() {

                        public Object doInStatement(Statement stmt) throws SQLException, DataAccessException {
                            ResultSet rs = stmt.executeQuery("select id,name from tddl_test_0000");
                            try {
                                assertEquals(true, rs.next());
                                assertEquals(10, rs.getInt(1));
                                assertEquals("str2", rs.getString(2));
                            } catch (SQLException e) {
                                Assert.fail(ExceptionUtils.getFullStackTrace(e));
                            }

                            return null;
                        }
                    });

                    throw new RuntimeException("rollback");
                    // return null;
                }
            });
        } catch (RuntimeException e) {
            // 检查一下数据库记录
        }

        jdbcTemplate.execute(new StatementCallback() {

            public Object doInStatement(Statement stmt) throws SQLException, DataAccessException {
                ResultSet rs = stmt.executeQuery("select id,name from tddl_test_0000");
                try {
                    assertEquals(false, rs.next());
                } catch (SQLException e) {
                    Assert.fail(ExceptionUtils.getFullStackTrace(e));
                }

                return null;
            }
        });

    }
}
