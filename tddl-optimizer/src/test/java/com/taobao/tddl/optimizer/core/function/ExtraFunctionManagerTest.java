package com.taobao.tddl.optimizer.core.function;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.core.function.ExtraFunctionManager;
import com.taobao.tddl.optimizer.core.function.aggregate.Sum;

/**
 * @author jianghang 2013-11-8 下午7:44:27
 * @since 5.1.0
 */
public class ExtraFunctionManagerTest {

    @Test
    public void testSimple() {
        IExtraFunction func = ExtraFunctionManager.getExtraFunction("SUM");
        Assert.assertEquals(Sum.class, func.getClass());
    }

    @Test
    public void test_extension() {
        IExtraFunction func = ExtraFunctionManager.getExtraFunction("custom");
        Assert.assertEquals(Custom.class, func.getClass());
    }
}
