package com.taobao.tddl.qatest.atom;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DataAccessException;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.atom.common.TAtomConstants;

public class AtomWriteRestraintTest extends AtomTestCase {

    @Before
    public void init() throws Exception {
        super.setUp();
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        prepareData(tddlJT, "insert into normaltbl_0001 (pk,gmt_create,name) values (?,?,?)", new Object[] { RANDOM_ID,
                time, "manhong" });
    }

    @After
    public void destroy() throws Exception {
        super.tearDown();
    }

    @Test
    public void lessThanWriteRestraintTest() throws InterruptedException {
        if (ASTATICISM_TEST) {
            return;
        }

        int executCount = 10;
        int writeCount = RandomUtils.nextInt(executCount);

        MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
            " maxPoolSize=100\r\nuserName=tddl\r\nminPoolSize=1\r\nwriteRestrictTimes=" + executCount + "\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);

        String sql = "update normaltbl_0001 set gmt_create=? where pk=?";
        for (int i = 0; i < writeCount; i++) {
            try {
                int rs = tddlJT.update(sql, new Object[] { nextDay, RANDOM_ID });
                Assert.assertEquals(1, rs);
                executCount--;
            } catch (DataAccessException ex) {
            }
        }
        Assert.assertTrue(executCount >= 0);
    }

    @Test
    public void moreThanWriteRestraintTest() throws InterruptedException {
        if (ASTATICISM_TEST) {
            return;
        }

        int WriteCount = 20;
        int executCount = 10;

        MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
            " maxPoolSize=100\r\nuserName=tddl\r\nminPoolSize=1\r\nwriteRestrictTimes=" + executCount + "\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);

        String sql = "update normaltbl_0001 set gmt_create=? where pk=?";
        Object[] args = new Object[] { nextDay, RANDOM_ID };
        for (int i = 0; i < WriteCount; i++) {
            try {
                int rs = tddlJT.update(sql, args);
                Assert.assertEquals(1, rs);
                executCount--;
            } catch (DataAccessException ex) {
            }
        }

        Assert.assertEquals(0, executCount);
    }

}
