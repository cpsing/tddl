package com.taobao.tddl.qatest.matrix.template;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.taobao.tddl.qatest.BaseTemplateTestCase;

/**
 * Comment for TransactionTemplateTest
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2013-4-10 上午10:43:54
 */

public class TransactionTemplateTest extends BaseTemplateTestCase {

    private DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(us);
    private TransactionTemplate          andorTransaction   = new TransactionTemplate(transactionManager);

    @Test
    public void transactionTest() throws Exception {
        andorTransaction.execute(new TransactionCallback() {

            @Override
            public Object doInTransaction(TransactionStatus status) {
                try {
                    sql = String.format("insert into %s (pk,name) values(?,?)", normaltblTableName);
                    andorJT.update(sql, new Object[] { RANDOM_ID, name });
                    sql = String.format("select * from %s where pk= ?", normaltblTableName);
                    Map re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
                    Assert.assertEquals(name, String.valueOf(re.get("NAME")));
                } catch (Exception ex) {
                    status.setRollbackOnly();
                }
                return null;
            }
        });
        sql = String.format("select * from %s where pk= ?", normaltblTableName);
        Map re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(name, String.valueOf(re.get("NAME")));
    }

    @Test
    public void rollbackTest() throws Exception {
        andorTransaction.execute(new TransactionCallback() {

            @Override
            public Object doInTransaction(TransactionStatus status) {
                try {
                    sql = String.format("insert into %s (pk,name) values(?,?)", normaltblTableName);
                    andorJT.update(sql, new Object[] { RANDOM_ID, name });
                    sql = String.format("select * from %s where pk= ?", normaltblTableName);
                    Map re = andorJT.queryForMap(sql, new Object[] { RANDOM_ID });
                    Assert.assertEquals(name, String.valueOf(re.get("NAME")));
                    status.setRollbackOnly();
                } catch (Exception ex) {
                    status.setRollbackOnly();
                }
                return null;
            }
        });
        // 验证查询不到数据
        sql = String.format("select * from %s where pk= ?", normaltblTableName);
        List le = andorJT.queryForList(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(0, le.size());
    }
}
