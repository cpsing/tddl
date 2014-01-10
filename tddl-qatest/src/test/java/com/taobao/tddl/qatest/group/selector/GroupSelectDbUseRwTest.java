package com.taobao.tddl.qatest.group.selector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.common.GroupDataSourceRouteHelper;
import com.taobao.tddl.qatest.group.GroupTestCase;

/**
 * Comment for GroupReadOnlyDSSelectTest
 * <p/>
 * Created Date: 2010-12-8 下午07:34:32
 */
@SuppressWarnings("rawtypes")
public class GroupSelectDbUseRwTest extends GroupTestCase {

    @BeforeClass
    public static void setUp() throws Exception {

    }

    @Before
    public void init() throws Exception {
        super.setUp();
        super.init();
    }

    @Test
    public void queryFromReadOnlyDSTest() throws InterruptedException {
        // 插入同样的数据到3个库
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        int rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertTrue(rs > 0);

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertTrue(rs > 0);

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
        rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertTrue(rs > 0);

        // 因为3个库都有数据所以无论查几次都能查到数据
        for (int i = 0; i < 6; i++) {
            Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
        }

        /*
         * 为了验证3个库都有读取到 将写库NA掉
         * qatest_normal_0:NA,qatest_normal_0_bac:r,qatest_normal_1_bac:r
         * (确保推送成功)
         */
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:NA,qatest_normal_0_bac:r,qatest_normal_1_bac:NA");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }
        // 仍然可以查询到数据
        for (int i = 0; i < 6; i++) {
            Map rex = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.assertEquals(time, String.valueOf(rex.get("gmt_create")));
        }

        // 恢復((确保推送成功))
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:NA,qatest_normal_0_bac:NA,qatest_normal_1_bac:r");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 仍然可以查询到数据
        for (int i = 0; i < 6; i++) {
            Map rex = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.assertEquals(time, String.valueOf(rex.get("gmt_create")));
        }
    }

    @Test
    public void queryFromWriteOnlyDSTest() throws InterruptedException {
        // 将3个库全部设置为只写 qatest_normal_0:w,qatest_normal_0_bac:w(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:w,qatest_normal_0_bac:w,qatest_normal_1_bac:w");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 插入同样的数据到3个库
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        int rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertTrue(rs > 0);

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertTrue(rs > 0);

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
        rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertTrue(rs > 0);

        // 因为3个库都是写库，所以查询的时候抛NoMoreDataSourceException异常
        for (int i = 0; i < 6; i++) {
            try {
                tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().indexOf("com.taobao.tddl.group.exception.NoMoreDataSourceException") != -1);
            }
        }
    }

    @Test
    public void updateFromWriteDSTest() {
        // 插入数据
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        int rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertTrue(rs > 0);

        // 写库上肯定能查到数据
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        // 读库上肯定没有数据(因为没有复制，也就说明准确地插入到了写库中)
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        List rsList = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(0, rsList.size());
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
        rsList = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(0, rsList.size());

        // 修改数据
        String sql1 = "update normaltbl_0001 set gmt_create=? where pk=?";
        rs = tddlJT.update(sql1, new Object[] { nextDay, RANDOM_ID });
        Assert.assertTrue(rs > 0);

        // 验证数据修改准确性
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        Map re4 = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re4.get("gmt_create")));

        // 读库上肯定没有数据(因为没有复制，也就说明准确地插入到了写库中)
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        rsList = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(0, rsList.size());
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
        rsList = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(0, rsList.size());

        // 删除数据
        String sql2 = "delete from normaltbl_0001 where pk=?";
        rs = tddlJT.update(sql2, new Object[] { RANDOM_ID });
        Assert.assertTrue(rs > 0);

        // 删除后肯定查不到数据
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        List re5 = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(0, re5.size());
    }

    @Test
    public void updateFromReadOnlyDSTest() throws InterruptedException {
        // 将3个库全部设置为只读(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:r,qatest_normal_0_bac:r,qatest_normal_1_bac:r");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 插入数据一定失败
        try {
            String sql = "update normaltbl_0001 set gmt_create=? where pk=?";
            tddlJT.update(sql, new Object[] { nextDay, RANDOM_ID });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().indexOf("com.taobao.tddl.group.exception.NoMoreDataSourceException") != -1);
        }

        // 更新数据一定失败
        try {
            String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
            tddlJT.update(sql, new Object[] { RANDOM_ID, time });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().indexOf("com.taobao.tddl.group.exception.NoMoreDataSourceException") != -1);
        }
    }

    @Test
    public void updateFromReadOnlyDSByDatasourceIndexTest() throws InterruptedException {
        // 将3个库全部设置为只读(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:r,qatest_normal_0_bac:r,qatest_normal_1_bac:r");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 插入数据
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        int rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertTrue(rs > 0);

        sql = "select * from normaltbl_0001 where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        Map re = tddlJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        // 更新数据
        sql = "update normaltbl_0001 set gmt_create=? where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        rs = tddlJT.update(sql, new Object[] { nextDay, RANDOM_ID });
        Assert.assertTrue(rs > 0);

        sql = "select * from normaltbl_0001 where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        re = tddlJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));
    }

    @Test
    public void wireteDataWithoutRWTest() throws Exception {
        // 将3个库全部设置为只读(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(), "qatest_normal_0,qatest_normal_0_bac,qatest_normal_1_bac");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 保证数据更新成功
        int rs = tddlJT.update("insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID,
                time });
        Assert.assertTrue(rs > 0);

        // 将3个库全部设置为只读(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=R");
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0_BAC),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0_bac\r\ndbType=mysql\r\ndbStatus=R");
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_1_BAC),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_1_bac\r\ndbType=mysql\r\ndbStatus=R");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 插入数据一定失败
        try {
            String sql = "update normaltbl_0001 set gmt_create=? where pk=?";
            tddlJT.update(sql, new Object[] { nextDay, RANDOM_ID });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().indexOf("com.taobao.tddl.group.exception.NoMoreDataSourceException") != -1);
        }

        // 恢复(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=WR");
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0_BAC),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0_bac\r\ndbType=mysql\r\ndbStatus=WR");
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_1_BAC),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_1_bac\r\ndbType=mysql\r\ndbStatus=WR");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }
    }

    @Test
    public void wireteDataWithRWTest_add() throws InterruptedException {
        // 将2个库全部设置为只读,1个为读写(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:r,qatest_normal_0_bac:wr,qatest_normal_1_bac:r");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 插入数据
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        int rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertTrue(rs > 0);

        sql = "select * from normaltbl_0001 where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        Map re = tddlJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        // 更新数据
        sql = "update normaltbl_0001 set gmt_create=? where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        rs = tddlJT.update(sql, new Object[] { nextDay, RANDOM_ID });
        Assert.assertTrue(rs > 0);

        sql = "select * from normaltbl_0001 where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        re = tddlJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));
    }

    @Test
    public void wireteDataWithRWTest_add2() throws InterruptedException {
        // 将1个库设置为读写(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:r,qatest_normal_0_bac:wr,qatest_normal_1_bac:r");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 插入数据
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        int rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertTrue(rs > 0);

        sql = "select * from normaltbl_0001 where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        Map re = tddlJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        // jdbc验证方式
        DataSource ds = fixDataSource.getSlaveDsByIndex(0);
        JdbcTemplate jt = new JdbcTemplate(ds);
        sql = "select * from normaltbl_0001 where pk=?";
        re = jt.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        // 更新数据
        sql = "update normaltbl_0001 set gmt_create=? where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        rs = tddlJT.update(sql, new Object[] { nextDay, RANDOM_ID });
        Assert.assertTrue(rs > 0);

        sql = "select * from normaltbl_0001 where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        re = tddlJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));

        // jdbc验证方式
        ds = fixDataSource.getSlaveDsByIndex(0);
        jt = new JdbcTemplate(ds);
        sql = "select * from normaltbl_0001 where pk=?";
        re = jt.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));
    }

    @Test
    public void wireteDataWithRWTest_add3() throws InterruptedException {
        // 将1个库设置为读写，写权重为0(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:r,qatest_normal_0_bac:w0r,qatest_normal_1_bac:r");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 插入数据一定失败
        try {
            String sql = "update normaltbl_0001 set gmt_create=? where pk=?";
            tddlJT.update(sql, new Object[] { nextDay, RANDOM_ID });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().indexOf("com.taobao.tddl.group.exception.NoMoreDataSourceException") != -1);
        }

        // 更新数据一定失败
        try {
            String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
            tddlJT.update(sql, new Object[] { RANDOM_ID, time });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().indexOf("com.taobao.tddl.group.exception.NoMoreDataSourceException") != -1);
        }
    }

    @Test
    public void wireteDataWithRWTest_add4() throws InterruptedException {
        // 将1个库设置为读写，1个为NA(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:NA,qatest_normal_0_bac:wr,qatest_normal_1_bac:r");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 插入数据
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        int rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertTrue(rs > 0);

        sql = "select * from normaltbl_0001 where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        Map re = tddlJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        // jdbc验证方式
        DataSource ds = fixDataSource.getSlaveDsByIndex(0);
        JdbcTemplate jt = new JdbcTemplate(ds);
        sql = "select * from normaltbl_0001 where pk=?";
        re = jt.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        // 更新数据
        sql = "update normaltbl_0001 set gmt_create=? where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        rs = tddlJT.update(sql, new Object[] { nextDay, RANDOM_ID });
        Assert.assertTrue(rs > 0);

        sql = "select * from normaltbl_0001 where pk=?";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        re = tddlJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));

        // jdbc验证方式
        ds = fixDataSource.getSlaveDsByIndex(0);
        jt = new JdbcTemplate(ds);
        sql = "select * from normaltbl_0001 where pk=?";
        re = jt.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));
    }
}
