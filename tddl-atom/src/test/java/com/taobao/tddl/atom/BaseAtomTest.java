package com.taobao.tddl.atom;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.taobao.diamond.mockserver.MockServer;

public class BaseAtomTest {

    @BeforeClass
    public static void beforeClass() {
        MockServer.setUpMockServer();
    }

    @AfterClass
    public static void after() {
        MockServer.tearDownMockServer();
    }
}
