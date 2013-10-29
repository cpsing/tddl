package com.taobao.tddl.rule.enumerator.handler;

public class IntegerPartDiscontinousRangeEnumerator extends NumberPartDiscontinousRangeEnumerator {

    @Override
    protected Number cast2Number(Comparable begin) {
        return (Integer) begin;
    }

    @Override
    protected Number getNumber(Comparable begin) {
        return (Integer) begin;
    }

    @Override
    protected Number plus(Number begin, int plus) {
        return (Integer) begin + plus;
    }

}
