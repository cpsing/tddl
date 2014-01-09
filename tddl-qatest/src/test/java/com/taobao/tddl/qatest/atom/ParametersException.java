package com.taobao.tddl.qatest.atom;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.tddl.atom.TAtomDataSource;

public class ParametersException extends AtomTestCase {

    @BeforeClass
    public static void setUp() throws Exception {

    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

    @Before
    public void init() throws Exception {

    }

    @After
    public void destroy() {

    }

    @Test
    public void appNameIsNullOrEmptyTest() throws Exception {
        tds = new TAtomDataSource();
        tds.setAppName("");
        tds.setDbKey(DBKEY_0);
        try {
            tds.init();
            Assert.fail("AppName null.");
        } catch (Exception e) {
        }
        // AppNameΪnullʱ
        tds = new TAtomDataSource();
        tds.setAppName(null);
        tds.setDbKey(DBKEY_0);
        try {
            tds.init();
            Assert.fail("AppName null.");
        } catch (Exception e) {
        }
    }

    @Test
    public void dbKeyIsNullOrEmptyTest() throws Exception {
        tds = new TAtomDataSource();
        tds.setAppName(APPNAME);
        tds.setDbKey("");
        try {
            tds.init();
            Assert.fail("dbKey null");
        } catch (Exception e) {
        }
        // dbKeyΪnullʱ
        tds = new TAtomDataSource();
        tds.setAppName(APPNAME);
        tds.setDbKey(null);
        try {
            tds.init();
            Assert.fail("dbKey null.");
        } catch (Exception e) {
        }
    }
}
