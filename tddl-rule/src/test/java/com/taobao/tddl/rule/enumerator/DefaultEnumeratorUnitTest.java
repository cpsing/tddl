package com.taobao.tddl.rule.enumerator;

import static com.taobao.tddl.rule.TestUtils.Equivalent;
import static com.taobao.tddl.rule.TestUtils.GreaterThan;
import static com.taobao.tddl.rule.TestUtils.LessThan;
import static com.taobao.tddl.rule.TestUtils.NotEquivalent;
import static com.taobao.tddl.rule.TestUtils.gand;
import static com.taobao.tddl.rule.TestUtils.gcomp;
import static com.taobao.tddl.rule.TestUtils.gor;
import static com.taobao.tddl.rule.TestUtils.testSet;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.rule.exceptions.TddlRuleException;
import com.taobao.tddl.rule.model.sqljep.Comparative;

/**
 * 对默认的枚举器做测试
 * 
 * @author shenxun
 */
public class DefaultEnumeratorUnitTest {

    Enumerator  dE                         = new EnumeratorImp();

    boolean     needMergeValueInCloseRange = true;
    Comparative beTestComparative          = null;

    // Test:getEnumeratedValue 测试等于 Or comparable null > <
    @Test
    public void testGetEnumeratedValue_等于() throws Exception {
        beTestComparative = gcomp(1, Equivalent);
        Set<Object> s = dE.getEnumeratedValue(beTestComparative, null, null, needMergeValueInCloseRange);
        testSet(new Object[] { 1 }, s);
    }

    @Test
    public void testGetEnumeratedValue_or() throws Exception {
        beTestComparative = gor(gor(gcomp(1, Equivalent), gcomp(2, Equivalent)), gcomp(3, Equivalent));
        Set<Object> s = dE.getEnumeratedValue(beTestComparative, null, null, needMergeValueInCloseRange);
        testSet(new Object[] { 1, 2, 3 }, s);
    }

    @Test
    public void testGetEnumberatedValue_comparable() throws Exception {
        Comparable c = 1;
        Set<Object> s = dE.getEnumeratedValue(c, null, null, needMergeValueInCloseRange);
        testSet(new Object[] { 1 }, s);
    }

    @Test
    public void testGetEnumberatedValue_null() throws Exception {
        Set<Object> s = dE.getEnumeratedValue(null, null, null, needMergeValueInCloseRange);
        testSet(new Object[] { null }, s);
    }

    @Test
    public void testGetEnumberatedValue_GreaterThan() throws Exception {
        beTestComparative = gcomp(1, GreaterThan);
        try {
            Set<Object> s = dE.getEnumeratedValue(beTestComparative, null, null, needMergeValueInCloseRange);
            System.out.println(s.size());
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertEquals("在没有提供叠加次数的前提下，不能够根据当前范围条件选出对应的定义域的枚举值，sql中不要出现> < >= <=", e.getMessage());
        }
    }

    @Test
    public void testCloseInterval() throws Exception {
        beTestComparative = gand(gcomp(1, GreaterThan), gcomp(4, LessThan));
        try {
            dE.getEnumeratedValue(beTestComparative, null, null, needMergeValueInCloseRange);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("当原子增参数或叠加参数为空时，不支持在sql中使用范围选择，如id>? and id<?", e.getMessage());
        }
    }

    @Test
    public void testGetEnumeratedValue_不等于() throws Exception {
        beTestComparative = gand(gcomp(1, GreaterThan), gcomp(5, NotEquivalent));
        try {
            dE.getEnumeratedValue(beTestComparative, 10, 1, needMergeValueInCloseRange);
        } catch (TddlRuleException e) {
            // 暂时不支持
        }
    }
}
