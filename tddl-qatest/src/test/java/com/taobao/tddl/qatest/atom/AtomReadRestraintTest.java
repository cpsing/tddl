package com.taobao.tddl.qatest.atom;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DataAccessException;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.atom.common.TAtomConstants;

public class AtomReadRestraintTest extends AtomTestCase {

    @Before
    public void init() throws Exception {
        super.setUp();
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        prepareData(tddlJT, "insert into normaltbl_0001 (pk,gmt_create,name) values (?,?,?)", new Object[] { RANDOM_ID,
                time, "manhong" });
    }

    @After
    public void destroy() throws Exception {
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        super.tearDown();
    }

    @Test
    public void lessThanReadRestraintByDynamicTest() throws InterruptedException {
        if (ASTATICISM_TEST) {
            return;
        }

        int executCount = 10;
        int readCount = RandomUtils.nextInt(executCount);

        MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
            " maxPoolSize=100\r\nuserName=tddl\r\nminPoolSize=1\r\nreadRestrictTimes=" + executCount + "\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);

        String sql = "select * from normaltbl_0001 where pk=?";
        for (int i = 0; i < readCount; i++) {
            try {
                Map rs = tddlJT.queryForMap(sql, new Object[] { RANDOM_ID });
                Assert.assertEquals(time, String.valueOf(rs.get("gmt_create")));
                executCount--;
            } catch (DataAccessException ex) {
            }
        }

        Assert.assertTrue(executCount >= 0);
    }

    @Test
    public void moreThanReadRestraintTest() throws InterruptedException {
        if (ASTATICISM_TEST) {
            return;
        }

        int readCount = 20;
        int executCount = 10;

        MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
            " maxPoolSize=100\r\nuserName=tddl\r\nminPoolSize=1\r\nreadRestrictTimes=" + executCount + "\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);

        String sql = "select * from normaltbl_0001 where pk=?";
        for (int i = 0; i < readCount; i++) {
            try {
                Map rs = tddlJT.queryForMap(sql, new Object[] { RANDOM_ID });
                Assert.assertEquals(time, String.valueOf(rs.get("gmt_create")));
                executCount--;
            } catch (DataAccessException ex) {
            }
        }
        Assert.assertEquals(0, executCount);
    }

    @Test
    public void lessThanReadRestraintByPropsTest() throws Exception {
        if (ASTATICISM_TEST) {
            return;
        }

        int readCount = 5;
        int executCount = 0;

        for (int i = 0; i < readCount; i++) {
            try {
                Map rs = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
                Assert.assertEquals(time, String.valueOf(rs.get("gmt_create")));
                executCount++;
            } catch (DataAccessException ex) {
            }
        }

        Assert.assertEquals(readCount, executCount);
    }
}
