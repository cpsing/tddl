package com.taobao.tddl.qatest.atom;

import java.util.HashMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.atom.TAtomDataSource;
import com.taobao.tddl.qatest.BaseAtomGroupTestCase;

public class AtomTestCase extends BaseAtomGroupTestCase {

    protected static JdbcTemplate    tddlJT;
    protected static TAtomDataSource tds;

    @BeforeClass
    public static void setUp() throws Exception {
        setAtomMockInfo(ATOM_NORMAL_0_PATH, APPNAME, DBKEY_0);
        tds = new TAtomDataSource();
        tds.setAppName(APPNAME);
        tds.setDbKey(DBKEY_0);
        tds.init();
        tddlJT = getJT();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        tds = null;
        tddlJT = null;
    }

    @Before
    public void init() throws Exception {
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
    }

    @After
    public void destroy() throws Exception {
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
    }

    protected static JdbcTemplate getJT() {
        return new JdbcTemplate(tds);
    }

    protected static JdbcTemplate getJT(String path, String appName, String dbKey) throws Exception {
        setAtomMockInfo(path, appName, dbKey);
        TAtomDataSource atomDs = new TAtomDataSource();
        atomDs.setAppName(appName);
        atomDs.setDbKey(dbKey);
        atomDs.init();
        return new JdbcTemplate(atomDs);
    }

    protected static void setAtomMockInfo(String path, String appName, String dbKey) throws Exception {
        dataMap = new HashMap<String, String>();

        // -----------------db1
        initAtomConfig(path, appName, dbKey);

        // -----------------MockServer
        MockServer.setConfigInfos(dataMap);
    }
}
