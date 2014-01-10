package com.taobao.tddl.qatest.group;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.common.GroupDataSourceRouteHelper;

public class ChangeMasterSlaveTest extends GroupTestCase {

    @BeforeClass
    public static void setUp() throws Exception {

    }

    @Before
    public void init() throws Exception {
        super.setUp();
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
    }

    @Test
    public void dynamicChangeMSDSTest() throws Exception {
        // 主备切换之前，正常执行一条sql
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        tddlJT.update(sql, new Object[] { RANDOM_ID, time });

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
        // 清除数据
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });

        // 主备切换(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:r,qatest_normal_0_bac:wr,qatest_normal_1_bac:r");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 主备切换之后，正常执行一条sql
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        int rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertEquals(1, rs);

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        // 清除数据
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });

        // 指定写库的dataSourceIndex
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
        sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        rs = tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        Assert.assertEquals(1, rs);
    }

    @Test
    public void dynamicAddMasterDSTest() throws Exception {
        // 主备切换之前，正常执行一条sql
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        tddlJT.update(sql, new Object[] { RANDOM_ID, time });

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
        // 清除数据
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });

        // 加库，并将写库转移到新加入的库
        dataMap = new HashMap<String, String>();
        initAtomConfig(ATOM_NORMAL_1_PATH, APPNAME, DBKEY_1); // 加库qatest_normal_1
        dataMap.put(tds.getFullDbGroupKey(),
            "qatest_normal_0:r,qatest_normal_0_bac:r,qatest_normal_1_bac:r,qatest_normal_1:wr");
        // 主备切换(确保推送成功)
        for (int i = 0; i < 3; i++) {
            MockServer.setConfigInfos(dataMap);
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 主备切换之后，正常执行一条sql
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        // 主备切换之后，正常执行一条sql
        tddlJT.update(sql, new Object[] { RANDOM_ID, time });

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(3);
        re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
    }

    @Test
    public void writeDataToReadOnlyDSTest() throws Exception {
        // 主备切换之前，正常执行一条sql
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        tddlJT.update(sql, new Object[] { RANDOM_ID, time });

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
        // 清除数据
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });

        // 修改为只读(确保推送成功)
        for (int i = 0; i < 3; i++) {
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=R");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 主备切换之后，正常执行一条sql
        try {
            clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().indexOf("com.taobao.tddl.group.exception.NoMoreDataSourceException") != -1);
        }

        // 恢复
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=WR");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
    }
}
