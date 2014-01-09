package com.taobao.tddl.qatest.group;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.atom.common.TAtomConstants;

/**
 * Comment for GroupRetryByRealExecuteTest
 * <p/>
 * Created Date: 2010-12-8 03:19:04
 */
public class GroupRetryExecuteTest extends GroupTestCase {

    private String sql = "select * from normaltbl_0000 where pk = 0";

    @Test
    public void oneOfAtomDssInGropuIsOkTest() throws Exception {
        // 改变group中的rw状态(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:NA,qatest_normal_0_bac:r,qatest_normal_1_bac:r");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        int successCnt = 0;
        for (int i = 0; i < 20; i++) {
            tddlJT.queryForList(sql);
            successCnt++;
        }
        Assert.assertEquals(20, successCnt);

        // qatest_normal_0状态改为只读(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=NA");
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:wr,qatest_normal_0_bac:r,qatest_normal_1_bac:r");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }

        successCnt = 0;
        for (int i = 0; i < 20; i++) {
            tddlJT.queryForList(sql);
            successCnt++;
        }
        Assert.assertEquals(20, successCnt);
    }

    @Test
    public void noneOfAtomDssInGropuIsOkTest() throws Exception {
        // 改变group中的rw状态(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:NA,qatest_normal_0_bac:NA,qatest_normal_1_bac:NA");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }
        for (int i = 0; i < 20; i++) {
            try {
                tddlJT.queryForList(sql);
                Assert.fail();
            } catch (Exception e) {
                System.out.println("qatest_normal_0's dbStatus=NA");
            }
        }

        // 改变atomDs的状态(确保推送成功)
        for (int i = 0; i < 2; i++) {
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=NA");
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0_BAC),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0_bac\r\ndbType=mysql\r\ndbStatus=NA");
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_1_BAC),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_1_bac\r\ndbType=mysql\r\ndbStatus=NA");
            MockServer.setConfigInfo(tds.getFullDbGroupKey(),
                "qatest_normal_0:wr,qatest_normal_0_bac:r,qatest_normal_1_bac:r");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
        }
        for (int i = 0; i < 20; i++) {
            try {
                tddlJT.queryForList(sql);
                Assert.fail();
            } catch (Exception e) {
                System.out.println("qatest_normal_0's dbStatus=NA");
            }
        }
    }
}
