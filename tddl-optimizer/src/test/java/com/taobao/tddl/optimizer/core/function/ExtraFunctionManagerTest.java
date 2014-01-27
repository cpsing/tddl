package com.taobao.tddl.optimizer.core.function;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.core.expression.ExtraFunctionManager;
import com.taobao.tddl.optimizer.core.expression.IExtraFunction;

/**
 * @author jianghang 2013-11-8 下午7:44:27
 * @since 5.0.0
 */
public class ExtraFunctionManagerTest {

    @Test
    public void test_extension() {
        IExtraFunction func = ExtraFunctionManager.getExtraFunction("DUMMY");
        Assert.assertEquals(DummyTest.class, func.getClass());

        func = ExtraFunctionManager.getExtraFunction("NOT_EXIST");
        Assert.assertEquals(DummyTest.class, func.getClass());
    }
}
