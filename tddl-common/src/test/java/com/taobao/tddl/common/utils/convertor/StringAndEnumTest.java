package com.taobao.tddl.common.utils.convertor;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author jianghang 2011-7-12 下午01:04:33
 */
public class StringAndEnumTest {

    private ConvertorHelper helper = new ConvertorHelper();

    @Test
    public void testStringAndEnum() {
        Convertor enumToString = helper.getConvertor(TestEnum.class, String.class);
        Convertor stringtoEnum = helper.getConvertor(String.class, TestEnum.class);
        String VALUE = "TWO";

        Object str = enumToString.convert(TestEnum.TWO, String.class); // 数字
        Assert.assertEquals(str, VALUE);
        Object enumobj = stringtoEnum.convert(VALUE, TestEnum.class); // BigDecimal
        Assert.assertEquals(enumobj, TestEnum.TWO);
    }

    public static enum TestEnum {
        ONE, TWO, THREE;
    }
}
