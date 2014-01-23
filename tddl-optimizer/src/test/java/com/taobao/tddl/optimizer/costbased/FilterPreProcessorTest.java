package com.taobao.tddl.optimizer.costbased;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.exceptions.EmptyResultFilterException;
import com.taobao.tddl.optimizer.utils.FilterUtils;

public class FilterPreProcessorTest extends BaseOptimizerTest {

    @Test
    public void test_路径短化_永真() {
        TableNode table = new TableNode("TABLE1");
        table.query("1=1 AND ID = 1");
        table.build();
        FilterPreProcessor.optimize(table, true);
        Assert.assertEquals(table.getWhereFilter().toString(), "TABLE1.ID = 1");
    }

    @Test
    public void test_路径短化_永真_多路() {
        TableNode table = new TableNode("TABLE1");
        table.query("(ID = 1 AND NAME = 'HELLO') OR (1=1) ");
        table.build();
        FilterPreProcessor.optimize(table, true);
        Assert.assertEquals(table.getWhereFilter().toString(), "1");
    }

    @Test
    public void test_路径短化_永假() {
        TableNode table = new TableNode("TABLE1");
        table.query("0=1 AND ID = 1");
        table.build();
        try {
            FilterPreProcessor.optimize(table, true);
            Assert.fail();
        } catch (EmptyResultFilterException e) {
            // 会出现EmptyResultFilterException异常
        }
    }

    @Test
    public void test_路径短化_永假_多路() {
        TableNode table = new TableNode("TABLE1");
        table.query("(ID = 1 AND NAME = 'HELLO') OR (0=1) ");
        table.build();
        FilterPreProcessor.optimize(table, true);
        Assert.assertEquals(table.getWhereFilter().toString(), "(TABLE1.ID = 1 AND TABLE1.NAME = HELLO)");
    }

    @Test
    public void test_路径短化_组合() {
        TableNode table = new TableNode("TABLE1");
        table.query("(1 = 1 AND (ID = 1 OR ID = 2) AND NAME = 'HELLO') OR (0=1 AND 1=1) OR (ID = 3)");
        table.build();
        FilterPreProcessor.optimize(table, true);
        System.out.println(FilterUtils.toDNFAndFlat(table.getWhereFilter()));
        Assert.assertEquals(table.getWhereFilter().toString(),
            "((TABLE1.ID = 1 AND TABLE1.NAME = HELLO) OR (TABLE1.ID = 2 AND TABLE1.NAME = HELLO) OR TABLE1.ID = 3)");
    }

    @Test
    public void test_列调整_常量() {
        TableNode table = new TableNode("TABLE1");
        table.query("1 = ID AND NAME = 'HELLO'");
        table.build();
        FilterPreProcessor.optimize(table, true);
        Assert.assertEquals(table.getWhereFilter().toString(), "(TABLE1.ID = 1 AND TABLE1.NAME = HELLO)");
    }

    @Test
    public void test_列调整_列值() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID = NAME AND 'HELLO'= NAME AND NAME = ID ");
        table.build();
        FilterPreProcessor.optimize(table, true);
        Assert.assertEquals(table.getWhereFilter().toString(),
            "(TABLE1.ID = TABLE1.NAME AND TABLE1.NAME = TABLE1.ID AND TABLE1.NAME = HELLO)");
    }

    @Test
    public void test_列调整_函数() {
        TableNode table = new TableNode("TABLE1");
        table.query("2 = ID+1");
        table.build();
        FilterPreProcessor.optimize(table, true);
        Assert.assertEquals(table.getWhereFilter().toString(), "ID + 1 = 2");
    }

    @Test
    public void test_列调整_各种组合() {
        TableNode table = new TableNode("TABLE1");
        table.query("HEX(2) = ID+1 AND NAME = HEX(NAME)");
        table.build();
        FilterPreProcessor.optimize(table, true);
        Assert.assertEquals(table.getWhereFilter().toString(), "(HEX(2) = ID + 1 AND TABLE1.NAME = HEX(NAME))");
    }

    @Test
    public void test_合并_AND条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID > 1 AND ID < 10");
        table.build();
        FilterPreProcessor.optimize(table, true);
        Assert.assertEquals(table.getWhereFilter().toString(), "(TABLE1.ID > 1 AND TABLE1.ID < 10)");

        table.query("ID > 1 AND ID < 10 AND ID >= 2 AND ID < 11");
        table.build();
        FilterPreProcessor.optimize(table, true);
        Assert.assertEquals(table.getWhereFilter().toString(), "(TABLE1.ID >= 2 AND TABLE1.ID < 10)");

        table.query("ID > 1 AND ID < 0");
        table.build();
        try {
            FilterPreProcessor.optimize(table, true);
            Assert.fail();
        } catch (EmptyResultFilterException e) {
            // 会出现EmptyResultFilterException异常
        }
    }

    @Test
    public void test_合并_OR条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID > 1 OR ID < 3");
        table.build();
        FilterPreProcessor.optimize(table, true);
        Assert.assertEquals(table.getWhereFilter().toString(), "1");

        table.query("ID > 1 OR ID > 3");
        table.build();
        FilterPreProcessor.optimize(table, true);
        Assert.assertEquals(table.getWhereFilter().toString(), "TABLE1.ID > 1");

        table.query("ID > 1 OR ID = 5");
        table.build();
        FilterPreProcessor.optimize(table, true);
        Assert.assertEquals(table.getWhereFilter().toString(), "TABLE1.ID > 1");
    }
}
