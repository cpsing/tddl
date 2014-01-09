package com.taobao.tddl.qatest.atom;

import java.util.Date;
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
import com.taobao.tddl.qatest.util.DateUtil;

public class AtomDynamicChangeAppTest extends AtomTestCase {

    @Before
    public void init() throws Exception {
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        prepareData(tddlJT, "insert into normaltbl_0001 (pk,gmt_create,name) values (?,?,?)", new Object[] { RANDOM_ID,
                time, "manhong" });
    }

    @After
    public void destroy() throws Exception {
        restore();
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
    }

    private void restore() throws Exception {
        MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
            "maxPoolSize=100\r\nuserName=tddl\r\nminPoolSize=1\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        Map re = null;
        try {
            re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        } catch (Exception ex) {
            MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
                "maxPoolSize=100\r\nuserName=tddl\r\nminPoolSize=1\r\n");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
            re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        }
        Assert.assertEquals("manhong", re.get("name"));
    }

    @Test
    public void dynamicChangeAppnameTest() throws Exception {
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
            "maxPoolSize=100\r\nuserName=xxxx\r\nminPoolSize=1\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);

        try {
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
                "maxPoolSize=100\r\nuserName=xxxx\r\nminPoolSize=1\r\n");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.fail();
        } catch (Exception ex) {
        }
    }

    @Test
    public void dynamicChangeMaxPoolTest() throws Exception {
        if (ASTATICISM_TEST) {
            return;
        }

        final CountDownLatch count = new CountDownLatch(1);
        final CountDownLatch count1 = new CountDownLatch(1);
        final AtomicInteger times = new AtomicInteger(0);
        final AtomicInteger times1 = new AtomicInteger(0);
        int exeuteCount = 12;
        final String sql = "select * from normaltbl_0001 where pk=?";
        final Object[] args = new Object[] { RANDOM_ID };
        final String time = DateUtil.formatDate(new Date(), DateUtil.DATE_FULLHYPHEN);
        ExecutorService es = Executors.newFixedThreadPool(20);

        for (int i = 0; i < exeuteCount; i++) {
            es.execute(new Runnable() {

                public void run() {
                    try {
                        count1.await();
                    } catch (InterruptedException e) {
                    }
                    try {
                        Map re = tddlJT.queryForMap(sql, args);
                        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
                        times1.incrementAndGet();
                    } catch (DataAccessException e) {
                    }
                }
            });
        }
        count1.countDown();
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
        }
        Assert.assertEquals(exeuteCount, times1.get());

        MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
            " maxPoolSize=3\r\nuserName=tddl\r\nminPoolSize=1\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);

        for (int i = 0; i < exeuteCount; i++) {
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
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
        }

        Assert.assertEquals(3, times.get());
    }

    @Test
    public void dynamicChangeMinPoolTest() throws Exception {
        if (ASTATICISM_TEST) {
            return;
        }

        final CountDownLatch count = new CountDownLatch(1);
        final CountDownLatch count1 = new CountDownLatch(1);
        final AtomicInteger times = new AtomicInteger(0);
        final AtomicInteger times1 = new AtomicInteger(0);
        int exeuteCount = 12;
        final String sql = "select * from normaltbl_0001 where pk=?";
        final Object[] args = new Object[] { RANDOM_ID };
        final String time = DateUtil.formatDate(new Date(), DateUtil.DATE_FULLHYPHEN);
        ExecutorService es = Executors.newFixedThreadPool(20);

        for (int i = 0; i < exeuteCount; i++) {
            es.execute(new Runnable() {

                public void run() {
                    try {
                        count1.await();
                    } catch (InterruptedException e) {
                    }
                    try {
                        Map re = tddlJT.queryForMap(sql, args);
                        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
                        times1.incrementAndGet();
                    } catch (DataAccessException e) {
                    }
                }
            });
        }
        count1.countDown();
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
        }
        Assert.assertEquals(exeuteCount, times1.get());

        MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
            " maxPoolSize=100\r\nuserName=tddl\r\nminPoolSize=10\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);

        for (int i = 0; i < exeuteCount; i++) {
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
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
        }
        Assert.assertEquals(exeuteCount, times.get());
    }

    @Test
    public void dynamicBlockingTimeoutTest() throws Exception {
        if (ASTATICISM_TEST) {
            return;
        }

        final CountDownLatch count = new CountDownLatch(1);
        final CountDownLatch count1 = new CountDownLatch(1);
        final AtomicInteger times = new AtomicInteger(0);
        final AtomicInteger times1 = new AtomicInteger(0);
        int exeuteCount = 12;
        final String sql = "select * from normaltbl_0001 where pk=?";
        final Object[] args = new Object[] { RANDOM_ID };
        final String time = DateUtil.formatDate(new Date(), DateUtil.DATE_FULLHYPHEN);
        ExecutorService es = Executors.newFixedThreadPool(20);

        MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
            "maxPoolSize=3\r\nuserName=tddl\r\nminPoolSize=1\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        for (int i = 0; i < exeuteCount; i++) {
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
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
        }
        Assert.assertEquals(3, times.get());

        MockServer.setConfigInfo(TAtomConstants.getAppDataId(APPNAME, DBKEY_0),
            "maxPoolSize=3\r\nuserName=tddl\r\nminPoolSize=1\r\nblockingTimeout=2000\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);

        for (int i = 0; i < exeuteCount; i++) {
            es.execute(new Runnable() {

                public void run() {
                    try {
                        count1.await();
                    } catch (InterruptedException e) {
                    }

                    try {
                        Map re = tddlJT.queryForMap(sql, args);
                        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
                        times1.incrementAndGet();
                    } catch (DataAccessException e) {
                    }
                }
            });
        }

        count1.countDown();
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
        }

        Assert.assertEquals(exeuteCount, times1.get());
    }
}
