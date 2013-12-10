package com.taobao.tddl.optimizer.costbased;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.costbased.pusher.OrderByPusher;

public class OrderByPusherTest extends BaseOptimizerTest {

    @Test
    public void test_order条件下推_子表() {
        TableNode table1 = new TableNode("TABLE1");
        table1.alias("A");

        QueryNode query = new QueryNode(table1);
        query.orderBy("A.ID");
        query.orderBy("A.NAME");
        query.build();

        OrderByPusher.optimize(query);

        System.out.println(query.getChild().getOrderBys().toString());
        Assert.assertEquals("TABLE1.ID > 5", query.getChild().getOrderBys().toString());
    }
}
