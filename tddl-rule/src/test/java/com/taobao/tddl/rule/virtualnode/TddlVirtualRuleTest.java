package com.taobao.tddl.rule.virtualnode;

import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.tddl.rule.TddlRule;
import com.taobao.tddl.rule.model.MatcherResult;
import com.taobao.tddl.rule.model.TargetDB;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-8-11 11:19:02
 */
public class TddlVirtualRuleTest {

    static TddlRule rule;
    static TddlRule rule_format;

    @BeforeClass
    public static void setUp() {
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:virtualnode/spring-context.xml");
        rule = (TddlRule) context.getBean("rule");
        rule_format = (TddlRule) context.getBean("rule-format");
    }

    @Test
    public void testTddlRule() {
        String conditionStr = "message_id in (996,997,998,999,1000,1001,1002,1003,1004):int";
        MatcherResult result = rule.route("nserch", conditionStr);
        List<TargetDB> dbs = result.getCalculationResult();
        Assert.assertEquals(2, dbs.size());
        StringBuilder sb = new StringBuilder("目标库:");
        sb.append(dbs.get(0).getDbIndex());
        sb.append(" 所要执行的表:");
        for (String table : dbs.get(0).getTableNames()) {
            sb.append(table);
            sb.append(" ");
        }

        Assert.assertEquals("目标库:NSEARCH_GROUP_2 所要执行的表:NSERCH_4 ", sb.toString());
    }

    @Test
    public void testTddlRuleFormat() {
        String conditionStr = "message_id in (996,997,998,999,1000,1001,1002,1003,1004):int";
        MatcherResult result = rule_format.route("nserch", conditionStr);
        List<TargetDB> dbs = result.getCalculationResult();
        Assert.assertEquals(2, dbs.size());
        StringBuilder sb = new StringBuilder("目标库:");
        sb.append(dbs.get(0).getDbIndex());
        sb.append(" 所要执行的表:");
        for (String table : dbs.get(0).getTableNames()) {
            sb.append(table);
            sb.append(" ");
        }

        Assert.assertEquals("目标库:NSEARCH_GROUP_2 所要执行的表:nserch_0004 ", sb.toString());
    }
}
