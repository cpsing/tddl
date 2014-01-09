package com.taobao.tddl.qatest.atom;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DataAccessException;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.atom.common.TAtomConstants;

public class AtomThreadRestraintTest extends AtomTestCase {

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
    public void lessThanThreadRestraintTest() throws InterruptedException {
        if (ASTATICISM_TEST) {
            return;
        }

        final CountDownLatch count = new CountDownLatch(1);
        final AtomicInteger times = new AtomicInteger(0);
        final String sql = "select * from normaltbl_0001 where pk=?";
        final Object[] args = new Object[] { RANDOM_ID };
        ExecutorService es = Executors.newFixedThreadPool(3);

        MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
            " maxPoolSize=100\r\nuserName=tddl\r\nminPoolSize=1\r\nthreadCountRestrict=5\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);

        for (int i = 0; i < 3; i++) {
            es.execute(new Runnable() {

                public void run() {
                    try {
                        count.await();
                    } catch (InterruptedException e) {
                    }

                    try {
                        Map re = tddlJT.queryForMap(sql, args);
                        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
                        times.incrementAndGet();
                    } catch (DataAccessException e) {
                    }
                }
            });
        }

        count.countDown();
        try {
            TimeUnit.MILLISECONDS.sleep(2000);
        } catch (InterruptedException e) {
        }
        Assert.assertEquals(3, times.get());
    }

    @Test
    public void moreThanThreadRestraintTest() throws InterruptedException {
        if (ASTATICISM_TEST) {
            return;
        }

        final CountDownLatch count = new CountDownLatch(1);
        final AtomicInteger times = new AtomicInteger(0);
        final String sql = "select * from normaltbl_0001 where pk=?";
        final Object[] args = new Object[] { RANDOM_ID };
        ExecutorService es = Executors.newFixedThreadPool(10);

        MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
            " maxPoolSize=100\r\nuserName=tddl\r\nminPoolSize=1\r\nthreadCountRestrict=2\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);

        for (int i = 0; i < 6; i++) {
            es.execute(new Runnable() {

                public void run() {
                    try {
                        count.await();
                    } catch (InterruptedException e) {
                        System.out.println("aaaaaa");
                    }

                    try {
                        Map re = tddlJT.queryForMap(sql, args);
                        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
                        times.incrementAndGet();
                    } catch (DataAccessException e) {
                    }
                }
            });
        }

        count.countDown();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
        Assert.assertEquals(2, times.get());
    }

}
