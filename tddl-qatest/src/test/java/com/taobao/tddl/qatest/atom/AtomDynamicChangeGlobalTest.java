package com.taobao.tddl.qatest.atom;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.atom.common.TAtomConstants;

public class AtomDynamicChangeGlobalTest extends AtomTestCase {

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
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        Map re = null;
        try {
            re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        } catch (Exception ex) {
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
            re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        }
        Assert.assertEquals("manhong", re.get("name"));
    }

    @Test
    public void dynamicChangeGlobalIpTest() throws InterruptedException {
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.13.40.25\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        try {
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=10.13.40.25\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.fail("");
        } catch (Exception ex) {
        }
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
    }

    @Test
    public void dynamicChangeGlobalPortTest() throws InterruptedException {
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3300\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");

        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        try {
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=10.232.31.154\r\nport=3300\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.fail();
        } catch (Exception ex) {
        }
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
    }

    @Test
    public void dynamicChangeGlobalDbNameTest() throws InterruptedException {
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal\r\ndbType=mysql\r\ndbStatus=RW\r\n");

        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        try {
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=\r\nport=3306\r\ndbName=qatest_normal\r\ndbType=mysql\r\ndbStatus=RW\r\n");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.fail();
        } catch (Exception ex) {
        }
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
    }

    @Ignore("oracle驱动暂时没依赖")
    @Test
    public void dynamicChangeGlobalDbTypeTest() throws InterruptedException {
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=oracle\r\ndbStatus=RW\r\n");

        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        try {
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=oracle\r\ndbStatus=RW\r\n");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.fail();
        } catch (Exception ex) {
        }
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
    }

    @Test
    public void dynamicChangeGlobalDbStatusToRTest() throws InterruptedException {
        String sql = "update normaltbl_0001  set gmt_create=? where pk=?";
        tddlJT.update(sql, new Object[] { nextDay, RANDOM_ID });
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=R\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));
        try {
            tddlJT.update("update normaltbl_0001 set gmt_create=? where pk=?", new Object[] { time, RANDOM_ID });
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=R\r\n");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
            tddlJT.update("update normaltbl_0001 set gmt_create=? where pk=?", new Object[] { time, RANDOM_ID });
            Assert.fail();
        } catch (Exception ex) {
        }
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);

        tddlJT.update("update normaltbl_0001 set gmt_create=? where pk=?", new Object[] { time, RANDOM_ID });
        re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

    }

    @Test
    public void dynamicChangeGlobalDbStatusToWTest() throws InterruptedException {
        String sql = "update normaltbl_0001  set gmt_create=? where pk=?";
        tddlJT.update(sql, new Object[] { nextDay, RANDOM_ID });
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=W\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        int result = tddlJT.update("update normaltbl_0001 set gmt_create=? where pk=?",
            new Object[] { time, RANDOM_ID });
        Assert.assertEquals(1, result);

        try {
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=W\r\n");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.fail();
        } catch (Exception ex) {
        }
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);

        tddlJT.update("update normaltbl_0001 set gmt_create=? where pk=?", new Object[] { nextDay, RANDOM_ID });
        re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));

    }

    @Test
    public void dynamicChangeGlobalDbStatusToNATest() throws InterruptedException {
        String sql = "update normaltbl_0001  set gmt_create=? where pk=?";
        tddlJT.update(sql, new Object[] { nextDay, RANDOM_ID });
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(nextDay, String.valueOf(re.get("gmt_create")));
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=NA\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        try {
            tddlJT.update("update normaltbl_0001 set gmt_create=? where pk=?", new Object[] { time, RANDOM_ID });
            MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
                "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=NA\r\n");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
            tddlJT.update("update normaltbl_0001 set gmt_create=? where pk=?", new Object[] { time, RANDOM_ID });
            Assert.fail();
        } catch (Exception ex) {
        }
        try {
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.fail();
        } catch (Exception ex) {
        }
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(DBKEY_0),
            "ip=10.232.31.154\r\nport=3306\r\ndbName=qatest_normal_0\r\ndbType=mysql\r\ndbStatus=RW\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
    }

}
