package com.taobao.tddl.atom;

import org.junit.After;
import org.junit.Before;

import com.taobao.diamond.mockserver.MockServer;

public class BaseAtomTest {

    @Before
    public void beforeClass() {
        MockServer.setUpMockServer();
    }

    @After
    public void after() {
        MockServer.tearDownMockServer();
    }
}
