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

@SuppressWarnings("rawtypes")
public class AtomDynamicChangePswTest extends AtomTestCase {

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
        MockServer.setConfigInfo(TAtomConstants.getPasswdDataId(DBKEY_0, DBTYPE_MYSQL, "tddl"),
            "encPasswd=4485f91c9426e4d8\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        Map re = null;
        try {
            re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        } catch (Exception ex) {
            MockServer.setConfigInfo(TAtomConstants.getPasswdDataId(DBKEY_0, DBTYPE_MYSQL, "tddl"),
                "encPasswd=4485f91c9426e4d8\r\n");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
            re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        }
        Assert.assertEquals("manhong", re.get("name"));
    }

    @Ignore("druid对passwd变更不会关闭老的正常的链接")
    @Test
    public void dynamicChangePswTest() throws Exception {
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));
        MockServer.setConfigInfo(TAtomConstants.getPasswdDataId(DBKEY_0, DBTYPE_MYSQL, "tddl"),
            "encPasswd=dddddddd\r\n");
        TimeUnit.SECONDS.sleep(SLEEP_TIME);
        try {
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            MockServer.setConfigInfo(TAtomConstants.getPasswdDataId(DBKEY_0, DBTYPE_MYSQL, "tddl"),
                "encPasswd=dddddddd\r\n");
            TimeUnit.SECONDS.sleep(SLEEP_TIME);
            tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
            Assert.fail();
        } catch (Exception ex) {
        }
    }
}
