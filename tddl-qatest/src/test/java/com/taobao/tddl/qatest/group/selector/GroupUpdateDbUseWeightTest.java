package com.taobao.tddl.qatest.group.selector;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.common.GroupDataSourceRouteHelper;
import com.taobao.tddl.qatest.group.GroupTestCase;

/**
 * Comment for GroupSelectDbUseWeightTest
 * <p/>
 * Created Date: 2010-12-9 上午11:38:16
 */
@SuppressWarnings("rawtypes")
public class GroupUpdateDbUseWeightTest extends GroupTestCase {

    private int operationCnt = 1000;

    @BeforeClass
    public static void setUp() throws Exception {
    }

    @Before
    public void init() throws Exception {
        super.setUp();
        super.init();
    }

    @Test
    public void updateDBWithoutRWTest() throws Exception {
        // 跳过本用例测试
        if (SOME_SHOULD_NOT_BE_TEST) {
            return;
        }

        // 将3个库全部设置为只读(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(), "qatest_normal_0,qatest_normal_0_bac,qatest_normal_1_bac");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            // 插入数据
            int rs = tddlJT.update("insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID,
                    time });
            Assert.assertTrue(rs > 0);

            GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
            List list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (list.size() == 1) {
                firstCnt++;
                GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
                clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            } else {
                GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
                list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                if (list.size() == 1) {
                    secondCnt++;
                    GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
                    clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                } else {
                    GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
                    list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                    if (list.size() == 1) {
                        thirdCnt++;
                        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
                        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                    } else {
                        Assert.fail("查不到数据。");
                    }
                }
            }
        }

        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 3);
        checkWeight(operationCnt, secondCnt, 1.0 / 3);
        checkWeight(operationCnt, thirdCnt, 1.0 / 3);
    }

    @Test
    public void updateDBUseBalanceWeightTest() throws Exception {
        // 跳过本用例测试
        if (SOME_SHOULD_NOT_BE_TEST) {
            return;
        }

        // 设置权重(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:w1r10,qatest_normal_0_bac:w1r10,qatest_normal_1_bac:w1r10");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int operationCnt = 1000;
        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            // 插入数据
            int rs = tddlJT.update("insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID,
                    time });
            Assert.assertTrue(rs > 0);

            // 确认更新的atomDS
            GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
            List list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (list.size() == 1) {
                firstCnt++;
                GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
                clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            } else {
                GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
                list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                if (list.size() == 1) {
                    secondCnt++;
                    GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
                    clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                } else {
                    GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
                    list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                    if (list.size() == 1) {
                        thirdCnt++;
                        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
                        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                    } else {
                        Assert.fail("查不到数据。");
                    }
                }
            }
        }

        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 3);
        checkWeight(operationCnt, secondCnt, 1.0 / 3);
        checkWeight(operationCnt, thirdCnt, 1.0 / 3);
    }

    @Test
    public void updateDBUseImbalanceWeightTest() throws Exception {
        // 跳过本用例测试
        if (SOME_SHOULD_NOT_BE_TEST) {
            return;
        }

        // 设置权重(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:w1r10,qatest_normal_0_bac:w2r10,qatest_normal_1_bac:w3r10");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int operationCnt = 1000;
        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            // 插入数据
            int rs = tddlJT.update("insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID,
                    time });
            Assert.assertTrue(rs > 0);

            // 确认更新的atomDS
            GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
            List list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (list.size() == 1) {
                firstCnt++;
                GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
                clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            } else {
                GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
                list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                if (list.size() == 1) {
                    secondCnt++;
                    GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
                    clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                } else {
                    GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
                    list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                    if (list.size() == 1) {
                        thirdCnt++;
                        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
                        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                    } else {
                        Assert.fail("查不到数据。");
                    }
                }
            }
        }

        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 6);
        checkWeight(operationCnt, secondCnt, 2.0 / 6);
        checkWeight(operationCnt, thirdCnt, 3.0 / 6);
    }

    @Test
    public void updateDBUseDefaultWeightTest() throws InterruptedException {
        // 跳过本用例测试
        if (SOME_SHOULD_NOT_BE_TEST) {
            return;
        }

        /* 默认权重(确保推送成功) */
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:wr10,qatest_normal_0_bac:wr10,qatest_normal_1_bac:wr10");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int operationCnt = 1000;
        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            // 插入数据
            int rs = tddlJT.update("insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID,
                    time });
            Assert.assertTrue(rs > 0);

            // 确认更新的atomDS
            GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
            List list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (list.size() == 1) {
                firstCnt++;
                GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
                clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            } else {
                GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
                list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                if (list.size() == 1) {
                    secondCnt++;
                    GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
                    clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                } else {
                    GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
                    list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                    if (list.size() == 1) {
                        thirdCnt++;
                        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
                        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                    } else {
                        Assert.fail("查不到数据。");
                    }
                }
            }
        }

        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 3);
        checkWeight(operationCnt, secondCnt, 1.0 / 3);
        checkWeight(operationCnt, thirdCnt, 1.0 / 3);
    }

    @Test
    public void someOfTheDssWeightAreZeroTest() throws InterruptedException {
        // 跳过本用例测试
        if (SOME_SHOULD_NOT_BE_TEST) {
            return;
        }

        // 设置权重(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:w1r10,qatest_normal_0_bac:w0r10,qatest_normal_1_bac:w3r10");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int operationCnt = 1000;
        int firstCnt = 0;
        int secondCnt = 0;
        int thirdCnt = 0;
        for (int i = 0; i < operationCnt; i++) {
            // 插入数据
            int rs = tddlJT.update("insert into normaltbl_0001 (pk,gmt_create) values (?,?)", new Object[] { RANDOM_ID,
                    time });
            Assert.assertTrue(rs > 0);

            // 确认更新的atomDS
            GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
            List list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            if (list.size() == 1) {
                firstCnt++;
                GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
                clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            } else {
                GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
                list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                if (list.size() == 1) {
                    secondCnt++;
                    GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
                    clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                } else {
                    GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
                    list = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                    if (list.size() == 1) {
                        thirdCnt++;
                        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
                        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                    } else {
                        Assert.fail("查不到数据。");
                    }
                }
            }
        }

        System.err.println("firstCnt=" + firstCnt + ", secondCnt=" + secondCnt + ", thirdCnt=" + thirdCnt);
        Assert.assertEquals(operationCnt, firstCnt + secondCnt + thirdCnt);
        Assert.assertEquals(0, secondCnt);
        checkWeight(operationCnt, firstCnt, 1.0 / 4);
        checkWeight(operationCnt, thirdCnt, 3.0 / 4);
    }
}
