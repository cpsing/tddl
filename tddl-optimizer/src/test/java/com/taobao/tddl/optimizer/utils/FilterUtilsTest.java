package com.taobao.tddl.optimizer.utils;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.exceptions.EmptyResultFilterException;

/**
 * @author jianghang 2013-11-13 下午4:57:52
 * @since 5.0.0
 */
public class FilterUtilsTest {

    @Test
    public void testDNF() {
        // ( ( A and D ) or B) and C
        IFilter filter = and(or(and(filter("A"), filter("D")), filter("B")), filter("C"));
        // 结果： ((A and D) and C) or (B and C)，没有拉平
        Assert.assertEquals(or(newAnd(and(filter("A"), filter("D")), filter("C")), and(filter("B"), filter("C"))),
            FilterUtils.toDNF(filter));

        // 结果： (A and D and C) or (B and C)， 拉平处理
        Assert.assertEquals(or(and(and(filter("A"), filter("D")), filter("C")), and(filter("B"), filter("C"))),
            FilterUtils.toDNFAndFlat(filter));

        Assert.assertEquals(Arrays.asList(Arrays.asList(filter("A"), filter("D"), filter("C")),
            Arrays.asList(filter("B"), filter("C"))),
            FilterUtils.toDNFNodesArray(FilterUtils.toDNF(filter)));
    }

    @Test
    public void testMerge() {
        try {
            // 1 < A < 10
            IFilter filter = and(filter("A", 1, OPERATION.GT), filter("A", 10, OPERATION.LT));
            Assert.assertEquals("(A > 1 AND A < 10)", FilterUtils.merge(filter).toString());

            // 1 < A <= 10, 2 <= A < 11
            filter = and(filter("A", 1, OPERATION.GT), filter("A", 10, OPERATION.LT_EQ));
            filter = newAnd(and(filter("A", 2, OPERATION.GT_EQ), filter("A", 11, OPERATION.LT)), filter);
            Assert.assertEquals("(A >= 2 AND A <= 10)", FilterUtils.merge(filter).toString());
        } catch (EmptyResultFilterException e) {
            Assert.fail();
        }

        try {
            // 1 < A , A < 0
            IFilter filter = and(filter("A", 1, OPERATION.GT), filter("A", 0, OPERATION.LT));
            FilterUtils.merge(filter);
            Assert.fail();// 不可能到这一步
        } catch (EmptyResultFilterException e) {
        }

        try {
            // A > 1 or A < 3
            IFilter filter = or(filter("A", 1, OPERATION.GT), filter("A", 3, OPERATION.LT));
            Assert.assertEquals("1", FilterUtils.merge(filter).toString());

            // A > 1 or A > 3
            filter = or(filter("A", 1, OPERATION.GT), filter("A", 3, OPERATION.GT));
            Assert.assertEquals("A > 1", FilterUtils.merge(filter).toString());

            // A > 1 or A = 5
            filter = or(filter("A", 1, OPERATION.GT), filter("A", 5, OPERATION.EQ));
            Assert.assertEquals("A > 1", FilterUtils.merge(filter).toString());
        } catch (EmptyResultFilterException e) {
            Assert.fail();
        }

    }

    @Test
    public void testCreateFilter() {
        String where = "id > 1 and id <= 10";
        IFilter filter = FilterUtils.createFilter(where);
        System.out.println(filter);

        where = "name = 'hello' and id in ('1','2')";
        filter = FilterUtils.createFilter(where);
        System.out.println(filter);

        where = "gmt_create = '2013-11-11 11:11:11'";
        filter = FilterUtils.createFilter(where);
        System.out.println(filter);

        where = "1+1";
        filter = FilterUtils.createFilter(where);
        System.out.println(filter);
    }

    private IFilter filter(Comparable column) {
        IBooleanFilter booleanFilter = ASTNodeFactory.getInstance().createBooleanFilter();
        booleanFilter.setColumn(column).setValue(1).setOperation(OPERATION.EQ);
        return booleanFilter;
    }

    private IFilter newAnd(IFilter left, IFilter right) {
        ILogicalFilter and = ASTNodeFactory.getInstance().createLogicalFilter().setOperation(OPERATION.AND);
        and.addSubFilter(left);
        and.addSubFilter(right);
        return and;
    }

    private IFilter filter(Comparable column, Comparable value, OPERATION op) {
        IBooleanFilter booleanFilter = ASTNodeFactory.getInstance().createBooleanFilter();
        booleanFilter.setColumn(ASTNodeFactory.getInstance().createColumn().setColumnName((String) column))
            .setValue(value)
            .setOperation(op);
        return booleanFilter;
    }

    private IFilter and(IFilter left, IFilter right) {
        return FilterUtils.and(left, right);
    }

    private IFilter or(IFilter left, IFilter right) {
        return FilterUtils.or(left, right);
    }
}
