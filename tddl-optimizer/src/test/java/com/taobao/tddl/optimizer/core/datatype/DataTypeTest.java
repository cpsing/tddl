package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Assert;
import org.junit.Test;

public class DataTypeTest {

    @Test
    public void testInteger() {
        DataType<Integer> dataType = DataType.IntegerType;

        Object obj = dataType.convertFrom(1);
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom(Byte.valueOf("1"));
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom(Short.valueOf("1"));
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom(Integer.valueOf("1"));
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom(Long.valueOf("1"));
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom(BigInteger.valueOf(1L));
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom(BigDecimal.valueOf(1L));
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom("1");
        Assert.assertEquals(Integer.valueOf(1), obj);
    }
}
