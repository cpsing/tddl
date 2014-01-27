package com.taobao.tddl.optimizer.utils;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.utils.range.AndRangeProcessor;
import com.taobao.tddl.optimizer.utils.range.OrRangeProcessor;

/**
 * @author jianghang 2013-11-13 下午4:57:57
 * @since 5.0.0
 */
public class RangeTest {

    private static final IColumn COLUMN = ASTNodeFactory.getInstance().createColumn().setColumnName("A");

    @Test
    public void testAnd() {
        // 1 < A < 10
        AndRangeProcessor andProcessor = new AndRangeProcessor(COLUMN);
        andProcessor.process(filter(COLUMN, 1, OPERATION.GT));
        andProcessor.process(filter(COLUMN, 10, OPERATION.LT));
        Assert.assertEquals(Arrays.asList(filter(COLUMN, 1, OPERATION.GT), filter(COLUMN, 10, OPERATION.LT)),
            andProcessor.toFilterList());

        // 1 < A <= 10, 2 <= A < 11
        andProcessor = new AndRangeProcessor(COLUMN);
        andProcessor.process(filter(COLUMN, 1, OPERATION.GT));
        andProcessor.process(filter(COLUMN, 10, OPERATION.LT_EQ));

        andProcessor.process(filter(COLUMN, 2, OPERATION.GT_EQ));
        andProcessor.process(filter(COLUMN, 11, OPERATION.LT));
        Assert.assertEquals(Arrays.asList(filter(COLUMN, 2, OPERATION.GT_EQ), filter(COLUMN, 10, OPERATION.LT_EQ)),
            andProcessor.toFilterList());

        // 1 < A , A < 0
        andProcessor = new AndRangeProcessor(COLUMN);
        andProcessor.process(filter(COLUMN, 1, OPERATION.GT));
        boolean failed = andProcessor.process(filter(COLUMN, 0, OPERATION.LT));
        Assert.assertEquals(false, failed);
    }

    @Test
    public void testOr() {
        // A > 1 or A < 3
        OrRangeProcessor orProcessor = new OrRangeProcessor(COLUMN);
        orProcessor.process(filter(COLUMN, 1, OPERATION.GT));
        orProcessor.process(filter(COLUMN, 3, OPERATION.LT));
        Assert.assertTrue(orProcessor.toFilterList().isEmpty());// full set

        // A > 1 or A > 3
        orProcessor = new OrRangeProcessor(COLUMN);
        orProcessor.process(filter(COLUMN, 1, OPERATION.GT));
        orProcessor.process(filter(COLUMN, 3, OPERATION.GT_EQ));
        Assert.assertEquals(Arrays.asList(Arrays.asList(filter(COLUMN, 1, OPERATION.GT))), orProcessor.toFilterList());

        // A > 1 or A = 5
        orProcessor = new OrRangeProcessor(COLUMN);
        orProcessor.process(filter(COLUMN, 1, OPERATION.GT));
        orProcessor.process(filter(COLUMN, 0, OPERATION.EQ));
        Assert.assertEquals(Arrays.asList(Arrays.asList(filter(COLUMN, 1, OPERATION.GT)),
            Arrays.asList(filter(COLUMN, 0, OPERATION.EQ))),
            orProcessor.toFilterList());
    }

    private IFilter filter(Comparable column, Comparable value, OPERATION op) {
        IBooleanFilter booleanFilter = ASTNodeFactory.getInstance().createBooleanFilter();
        booleanFilter.setColumn(column).setValue(value).setOperation(op);
        return booleanFilter;
    }

}
