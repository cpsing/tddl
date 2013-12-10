package com.taobao.tddl.optimizer.costbased;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;

public class JoinPreProcessorTest extends BaseOptimizerTest {

    @Test
    public void test_交换_右链接() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        JoinNode join = table1.join(table2).setRightOuterJoin();
        join.build();

        JoinPreProcessor.optimize(join);
        Assert.assertEquals(true, join.isLeftOuterJoin());
        Assert.assertEquals(table2, join.getLeftNode());
    }
}
