package com.taobao.tddl.group.exception;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author yangzhu
 */
public class TAtomDataSourceExceptionUnitTest {

    @Test
    public void all() {
        Throwable cause = new Throwable();
        String msg = "msg";
        TAtomDataSourceException e = new TAtomDataSourceException();
        e = new TAtomDataSourceException(msg);
        Assert.assertEquals(msg, e.getMessage());
        e = new TAtomDataSourceException(cause);
        Assert.assertEquals(cause, e.getCause());
        e = new TAtomDataSourceException(msg, cause);
        Assert.assertEquals(msg, e.getMessage());
        Assert.assertEquals(cause, e.getCause());
    }
}
