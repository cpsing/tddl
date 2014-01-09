package com.taobao.tddl.qatest.matrix.template;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.taobao.tddl.qatest.BaseTemplateTestCase;

/**
 * Comment for JDBCTemplateTest
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2013-4-9 下午6:25:13
 */
public class JDBCTemplateTest extends BaseTemplateTestCase {

    @Test
    public void CRUDTest() throws Exception {
        sql = String.format("insert into %s (pk,name) values(?,?)", normaltblTableName);
        andorJT.update(sql, new Object[] { RANDOM_ID, name });

        sql = String.format("select * from %s where pk= ?", normaltblTableName);
        Map re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(name, String.valueOf(re.get("NAME")));

        sql = String.format("update %s set name =? where pk=? ", normaltblTableName);
        andorJT.update(sql, new Object[] { name1, RANDOM_ID });

        sql = String.format("select * from %s where pk= ?", normaltblTableName);
        re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(name1, String.valueOf(re.get("NAME")));

        sql = String.format("delete from %s where pk = ?", normaltblTableName);
        andorJT.update(sql, new Object[] { RANDOM_ID });

        sql = String.format("select * from %s where pk= ?", normaltblTableName);
        List le = andorJT.queryForList(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(0, le.size());
    }

    @Test
    public void tractionCommitTest() {
        JdbcTemplate andorJT = new JdbcTemplate(us);
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(us);
        TransactionStatus ts = transactionManager.getTransaction(def);

        try {
            sql = String.format("insert into %s (pk,name) values(?,?)", normaltblTableName);
            andorJT.update(sql, new Object[] { RANDOM_ID, name });
            sql = String.format("select * from %s where pk= ?", normaltblTableName);
            Map re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
            Assert.assertEquals(name, String.valueOf(re.get("NAME")));
        } catch (DataAccessException ex) {
            transactionManager.rollback(ts);
            throw ex;
        } finally {
            transactionManager.commit(ts);
        }
        sql = String.format("select * from %s where pk= ?", normaltblTableName);
        Map re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(name, String.valueOf(re.get("NAME")));
    }

    @Test
    public void tractionRollBackTest() {
        JdbcTemplate andorJT = new JdbcTemplate(us);
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(us);
        TransactionStatus ts = transactionManager.getTransaction(def);

        try {
            sql = String.format("insert into %s (pk,name) values(?,?)", normaltblTableName);
            andorJT.update(sql, new Object[] { RANDOM_ID, name });
            sql = String.format("select * from %s where pk= ?", normaltblTableName);
            Map re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
            Assert.assertEquals(name, String.valueOf(re.get("NAME")));
            // 回滚
            transactionManager.rollback(ts);
        } catch (DataAccessException ex) {
            transactionManager.rollback(ts);
            throw ex;
        } finally {
        }
        // 验证查询不到数据
        sql = String.format("select * from %s where pk= ?", normaltblTableName);
        List le = andorJT.queryForList(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(0, le.size());
    }
}
