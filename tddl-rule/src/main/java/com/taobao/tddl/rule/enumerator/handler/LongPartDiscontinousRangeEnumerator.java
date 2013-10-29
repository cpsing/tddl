package com.taobao.tddl.rule.enumerator.handler;

import com.taobao.tddl.rule.enumerator.EnumeratorUtils;

public class LongPartDiscontinousRangeEnumerator extends NumberPartDiscontinousRangeEnumerator {

    @Override
    protected Number cast2Number(Comparable begin) {
        return (Long) begin;
    }

    @Override
    protected Number getNumber(Comparable begin) {
        return (Long) EnumeratorUtils.toPrimaryValue(begin);
    }

    @Override
    protected Number plus(Number begin, int plus) {
        return (Long) begin + plus;
    }
}
