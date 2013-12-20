package com.taobao.tddl.group.exception;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author yangzhu
 */
public class ConfigExceptionUnitTest {

    @Test
    public void all() {
        Throwable cause = new Throwable();
        String msg = "msg";
        ConfigException e = new ConfigException();
        e = new ConfigException(msg);
        Assert.assertEquals(msg, e.getMessage());
        e = new ConfigException(cause);
        Assert.assertEquals(cause, e.getCause());
        e = new ConfigException(msg, cause);
        Assert.assertEquals(msg, e.getMessage());
        Assert.assertEquals(cause, e.getCause());
    }
}
