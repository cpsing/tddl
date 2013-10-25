package com.tddl.tddl.monitor.logger;

import org.junit.Test;

import com.taobao.tddl.monitor.logger.LoggerInit;

public class DynamicLoggerTest {

    // @Test
    public void testLog4j() {
        System.setProperty("tddl.application.logger", "log4j");
        System.setProperty("user.home", System.getProperty("java.io.tmpdir", "/tmp"));
        System.out.println(LoggerInit.logger.getDelegate().getClass());
        LoggerInit.logger.error("this is test");
    }

    @Test
    public void testLogback() {
        System.setProperty("tddl.application.logger", "logback");
        System.setProperty("user.home", System.getProperty("java.io.tmpdir", "/tmp"));
        System.out.println(LoggerInit.logger.getDelegate().getClass());
        LoggerInit.logger.error("this is test");
    }
}
