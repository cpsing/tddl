package com.taobao.tddl.optimizer.core.datatype;

public interface Calculator {

    Object add(Object v1, Object v2);

    Object sub(Object v1, Object v2);

    Object multiply(Object v1, Object v2);

    Object divide(Object v1, Object v2);

    Object mod(Object v1, Object v2);

    Object and(Object v1, Object v2);

    Object or(Object v1, Object v2);

    Object xor(Object v1, Object v2);

    Object not(Object v1);

    Object bitAnd(Object v1, Object v2);

    Object bitOr(Object v1, Object v2);

    Object bitXor(Object v1, Object v2);

    Object bitNot(Object v1);
}
