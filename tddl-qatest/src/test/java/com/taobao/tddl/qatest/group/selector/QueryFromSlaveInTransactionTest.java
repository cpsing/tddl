package com.taobao.tddl.qatest.group.selector;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.taobao.tddl.common.GroupDataSourceRouteHelper;
import com.taobao.tddl.qatest.group.GroupTestCase;

/**
 * Comment for queryFromSlaveInTransactionTest
 * <p/>
 * Created Date: 2010-12-8 下午08:37:41
 */
@SuppressWarnings("rawtypes")
public class QueryFromSlaveInTransactionTest extends GroupTestCase {

    @Test
    public void querySlaveInTransactionTest() throws Exception {

        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(tds);
        TransactionTemplate transTemp = new TransactionTemplate();
        transTemp.setTransactionManager(transactionManager);

        // 插入不同的数据到2个库
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        Object[] arguments = new Object[] { RANDOM_ID, nextDay };
        tddlJT.update(sql, arguments);

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        String sql2 = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        tddlJT.update(sql2, new Object[] { RANDOM_ID, time });

        transTemp.execute(new TransactionCallback() {

            public Object doInTransaction(TransactionStatus status) {
                try {
                    // 事务中未指定数据库进行读写操作，必走写库
                    for (int i = 0; i < 10; i++) {
                        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?",
                            new Object[] { RANDOM_ID });
                        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));
                    }
                } catch (DataAccessException e) {
                    status.setRollbackOnly();
                    Assert.fail(e.getMessage());
                }
                return null;
            }
        });

        transTemp.execute(new TransactionCallback() {

            public Object doInTransaction(TransactionStatus status) {
                try {
                    // 事务中指定数据库进行读写操作，那么事务中进行读写必走写库的限制就没有了
                    GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
                    String sql3 = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
                    Object[] arguments3 = new Object[] { RANDOM_ID, time };
                    tddlJT.update(sql3, arguments3);

                    GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
                    Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                    Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
                } catch (DataAccessException e) {
                    status.setRollbackOnly();
                    Assert.fail(e.getMessage());
                }
                return null;
            }
        });
    }
}
