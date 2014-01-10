package com.taobao.tddl.qatest.group.selector;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.dao.DataAccessException;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.common.GroupDataSourceRouteHelper;
import com.taobao.tddl.qatest.group.GroupTestCase;
import com.taobao.tddl.qatest.util.DateUtil;

/**
 * Comment for GroupSelectDbUseWeightTest
 * <p/>
 * Created Date: 2010-12-9 上午11:38:16
 */
@SuppressWarnings("rawtypes")
public class GroupSelectDbUseWeightTest extends GroupTestCase {

    private String theDayAfterTomorow = DateUtil.getDiffDate(2, DateUtil.DATE_FULLHYPHEN);
    private int    operationCnt       = 1000;

    @BeforeClass
    public static void setUp() throws Exception {
    }

    @Before
    public void init() throws Exception {
        super.setUp();
        super.init();
        // 插入不同的数据到3个库
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        tddlJT.update(sql, new Object[] { RANDOM_ID, time });
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        tddlJT.update(sql, new Object[] { RANDOM_ID, nextDay });
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
        tddlJT.update(sql, new Object[] { RANDOM_ID, theDayAfterTomorow });
    }

    @Test
    public void selectDBUseWeightTest() throws InterruptedException {
        // qatest_normal_0:w0r0,qatest_normal_0_bac:r0,qatest_normal_1:r10，肯定会读到qatest_normal_1库(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:wr0,qatest_normal_0_bac:r0,qatest_normal_1_bac:r10");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 肯定查到qatest_normal_1_bac库
        Map rex = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(theDayAfterTomorow, String.valueOf(rex.get("gmt_create")));

        // 三个数据源R全变成0(确保推送成功)
        for (int i = 0; i < 2; i++) {

            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:wr0,qatest_normal_0_bac:r0,qatest_normal_1_bac:r0");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        // 3个数据源查询全为0，肯定查不到任何数据，肯定抛异常
        try {
            tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.fail();
        } catch (DataAccessException e1) {

        }
    }

    @Test
    public void defaultWeightTest() throws InterruptedException {
        /* 默认权重 */
        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            Map rex = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (time.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                firstCnt++;
            } else if (nextDay.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                secondCnt++;
            } else if (theDayAfterTomorow.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                thirdCnt++;
            } else {
                Assert.fail("查询结果中出现不该有的数据。gmt_create = " + String.valueOf(rex.get("gmt_create")));
            }
        }

        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 3);
        checkWeight(operationCnt, secondCnt, 1.0 / 3);
        checkWeight(operationCnt, thirdCnt, 1.0 / 3);
    }

    @Test
    public void balanceWeightTest() throws InterruptedException {
        /* 均衡权重(确保推送成功) */
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:wr10,qatest_normal_0_bac:r10,qatest_normal_1_bac:r10");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            Map rex = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (time.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                firstCnt++;
            } else if (nextDay.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                secondCnt++;
            } else if (theDayAfterTomorow.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                thirdCnt++;
            } else {
                Assert.fail("查询结果中出现不该有的数据。gmt_create = " + String.valueOf(rex.get("gmt_create")));
            }
        }

        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 3);
        checkWeight(operationCnt, secondCnt, 1.0 / 3);
        checkWeight(operationCnt, thirdCnt, 1.0 / 3);
    }

    @Test
    public void imbalanceWeightTest() throws InterruptedException {
        /* 不等权重(确保推送成功) */
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:wr1,qatest_normal_0_bac:r2,qatest_normal_1_bac:r3");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            Map rex = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (time.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                firstCnt++;
            } else if (nextDay.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                secondCnt++;
            } else if (theDayAfterTomorow.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                thirdCnt++;
            } else {
                Assert.fail("查询结果中出现不该有的数据。gmt_create = " + String.valueOf(rex.get("gmt_create")));
            }
        }

        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        Assert.assertTrue(firstCnt < secondCnt && secondCnt < thirdCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 6);
        checkWeight(operationCnt, secondCnt, 2.0 / 6);
        checkWeight(operationCnt, thirdCnt, 3.0 / 6);
    }

    @Test
    public void someOfTheDssWeightAreZeroTest() throws InterruptedException {
        /* 0权重(确保推送成功) */
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:wr1,qatest_normal_0_bac:r0,qatest_normal_1_bac:r3");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            Map rex = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (time.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                firstCnt++;
            } else if (nextDay.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                secondCnt++;
            } else if (theDayAfterTomorow.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                thirdCnt++;
            } else {
                Assert.fail("查询结果中出现不该有的数据。gmt_create = " + String.valueOf(rex.get("gmt_create")));
            }
        }

        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        Assert.assertEquals(0, secondCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 4);
        checkWeight(operationCnt, thirdCnt, 3.0 / 4);
    }

    @Test
    public void someOfDssWithoutWeightTest() throws InterruptedException {
        /* 有部分DS没有设置权重值(确保推送成功) */
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:wr1,qatest_normal_0_bac:r,qatest_normal_1_bac:r3");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            Map rex = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (time.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                firstCnt++;
            } else if (nextDay.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                secondCnt++;
            } else if (theDayAfterTomorow.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                thirdCnt++;
            } else {
                Assert.fail("查询结果中出现不该有的数据。gmt_create = " + String.valueOf(rex.get("gmt_create")));
            }
        }

        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        // Assert.assertEquals(0, secondCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 14);
        checkWeight(operationCnt, secondCnt, 10.0 / 14);
        /**
         * 在权重设置过程中,有部分权重没有设置 , 那么默认情况下为10,这个设计不怎么合理 ,需要开发确认
         */
        checkWeight(operationCnt, thirdCnt, 3.0 / 14);

    }

    @Test
    public void someOfDssWithoutRWeightTest() throws InterruptedException {
        /* 有部分DS没有设置权重值(确保推送成功) */
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:wr1,qatest_normal_0_bac:w,qatest_normal_1_bac:r3");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            Map rex = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (time.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                firstCnt++;
            } else if (nextDay.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                secondCnt++;
            } else if (theDayAfterTomorow.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                thirdCnt++;
            } else {
                Assert.fail("查询结果中出现不该有的数据。gmt_create = " + String.valueOf(rex.get("gmt_create")));
            }
        }

        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        Assert.assertEquals(0, secondCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 4);
        checkWeight(operationCnt, thirdCnt, 3.0 / 4);
    }

    @Test
    public void someOfDssWithoutRWAndWeightTest() throws InterruptedException {
        /* 有部分DS没有设置权重值(确保推送成功) */
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(), "qatest_normal_0:wr1,,qatest_normal_1_bac:r3");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            Map rex = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (time.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                firstCnt++;
            } else if (nextDay.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                secondCnt++;
            } else if (theDayAfterTomorow.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                thirdCnt++;
            } else {
                Assert.fail("查询结果中出现不该有的数据。gmt_create = " + String.valueOf(rex.get("gmt_create")));
            }
        }

        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 3);
        checkWeight(operationCnt, secondCnt, 1.0 / 3);
        checkWeight(operationCnt, thirdCnt, 1.0 / 3);
    }

    @Test
    public void someOfDssWithoutRWAndWeightWhoDbstatusAreNATest() throws InterruptedException {
        /* 有部分DS没有设置权重值 */
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0_BAC),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0_bac\r\ndbType=mysql\r\ndbStatus=NA");
            MockServer.setConfigInfo(tds.getFullDbGroupKey(), "qatest_normal_0:wr1,,qatest_normal_1_bac:r3");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            Map rex = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (time.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                firstCnt++;
            } else if (nextDay.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                secondCnt++;
            } else if (theDayAfterTomorow.equalsIgnoreCase(String.valueOf(rex.get("gmt_create")))) {
                thirdCnt++;
            } else {
                Assert.fail("查询结果中出现不该有的数据。gmt_create = " + String.valueOf(rex.get("gmt_create")));
            }
        }

        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        Assert.assertEquals(0, secondCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 2);
        checkWeight(operationCnt, thirdCnt, 1.0 / 2);

        // 恢复配置(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0_BAC),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0_bac\r\ndbType=mysql\r\ndbStatus=RW");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }
    }
}
