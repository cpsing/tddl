package com.taobao.tddl.rule;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.collect.Lists;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.rule.BaseRuleTest.Choicer;
import com.taobao.tddl.rule.exceptions.RouteCompareDiffException;
import com.taobao.tddl.rule.exceptions.TddlRuleException;
import com.taobao.tddl.rule.model.MatcherResult;
import com.taobao.tddl.rule.model.TargetDB;
import com.taobao.tddl.rule.model.sqljep.Comparative;
import com.taobao.tddl.rule.utils.MatchResultCompare;

/**
 * 一些常见的本地rule测试
 * 
 * @author jianghang 2013-11-6 下午9:22:54
 * @since 5.1.0
 */
public class LocalRuleTest {

    static TddlRule rule;
    static TddlRule mvrRule;

    @BeforeClass
    public static void setUp() {
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:local/spring-context.xml");
        rule = (TddlRule) context.getBean("rule");
        mvrRule = (TddlRule) context.getBean("mvrRule");
    }

    @AfterClass
    public static void tearDown() throws TddlException {
        rule.destory();
        mvrRule.destory();
    }

    @Test
    public void testRule_equals() {
        MatcherResult result = rule.route("nserch", "message_id = 1");
        List<TargetDB> dbs = result.getCalculationResult();
        Assert.assertEquals(1, dbs.size());
        Assert.assertEquals("NSEARCH_GROUP_2", dbs.get(0).getDbIndex());
        Assert.assertEquals(1, dbs.get(0).getTableNames().size());
        Assert.assertEquals("nserch_1", dbs.get(0).getTableNames().iterator().next());
    }

    @Test
    public void testRule_in() {
        String conditionStr = "message_id in (996,997,998,999,1000,1001,1002,1003,1004):int";
        MatcherResult result = rule.route("nserch", conditionStr);
        List<TargetDB> dbs = result.getCalculationResult();
        Assert.assertEquals(3, dbs.size());
        StringBuilder sb = new StringBuilder("目标库:");
        sb.append(dbs.get(0).getDbIndex());
        sb.append(" 所要执行的表:");
        for (String table : dbs.get(0).getTableNames()) {
            sb.append(table);
            sb.append(" ");
        }

        Assert.assertEquals("目标库:NSEARCH_GROUP_1 所要执行的表:nserch_18 nserch_15 nserch_12 ", sb.toString());
    }

    @Test(expected = TddlRuleException.class)
    public void testRoute_noVersion() {
        // 不存在该version版本
        rule.route("nserch", "message_id = 1", "V1");
    }

    @Test
    public void testRule_Trim() {
        String conditionStr = "message_id in (996 ,997 , 998,999,1000 ,1001, 1002,1003,1004):int";
        MatcherResult result = rule.route("nserch", conditionStr);

        List<TargetDB> dbs = result.getCalculationResult();
        Assert.assertEquals(3, dbs.size());
        StringBuilder sb = new StringBuilder("目标库:");
        sb.append(dbs.get(0).getDbIndex());
        sb.append(" 所要执行的表:");
        for (String table : dbs.get(0).getTableNames()) {
            sb.append(table);
            sb.append(" ");
        }

        Assert.assertEquals("目标库:NSEARCH_GROUP_1 所要执行的表:nserch_18 nserch_15 nserch_12 ", sb.toString());
    }

    @Test
    public void testRule_date() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String conditionStr = "message_id >24:int and message_id<=26:int;gmt_create>=" + sdf.format(new Date())
                              + ":date";
        MatcherResult result = rule.route("nserch", conditionStr);
        Assert.assertEquals(true,
            MatchResultCompare.oriDbTabCompareWithMatchResult(result, "NSEARCH_GROUP_2", "nserch_1"));
    }

    @Test
    public void testRule_FullTableScan() {
        String conditionStr = "";
        MatcherResult result = rule.route("nserch", conditionStr);
        List<TargetDB> dbs = result.getCalculationResult();
        StringBuilder sb = new StringBuilder("目标库:");
        sb.append(dbs.get(0).getDbIndex());
        sb.append(" 所要执行的表:");
        for (String table : dbs.get(0).getTableNames()) {
            sb.append(table);
            sb.append(" ");
        }
        System.out.println(sb.toString());

        StringBuilder sb2 = new StringBuilder("目标库:");
        sb2.append(dbs.get(1).getDbIndex());
        sb2.append(" 所要执行的表:");
        for (String table : dbs.get(1).getTableNames()) {
            sb2.append(table);
            sb2.append(" ");
        }
        System.out.println(sb2.toString());

        StringBuilder sb3 = new StringBuilder("目标库:");
        sb3.append(dbs.get(2).getDbIndex());
        sb3.append(" 所要执行的表:");
        for (String table : dbs.get(2).getTableNames()) {
            sb3.append(table);
            sb3.append(" ");
        }
        System.out.println(sb3.toString());
    }

    @Test
    public void testRouteWithSpecifyRuleVersion() {
        MatcherResult result = mvrRule.route("nserch", "message_id = 1", "V1");
        List<TargetDB> dbs = result.getCalculationResult();
        Assert.assertEquals(1, dbs.size());
        Assert.assertEquals("NSEARCH_GROUP_2", dbs.get(0).getDbIndex());
        Assert.assertEquals(1, dbs.get(0).getTableNames().size());
        Assert.assertEquals("nserch_1", dbs.get(0).getTableNames().iterator().next());
    }

    @Test
    public void testRouteMultiVersionAndCompareTSqlTypeStringString() {
        Choicer choicer = new Choicer();
        choicer.addComparative("MESSAGE_ID", new Comparative(Comparative.Equivalent, 1)); // 一定要大写
        MatcherResult result = null;
        try {
            result = mvrRule.routeMverAndCompare(false, "nserch", choicer, Lists.newArrayList());
        } catch (RouteCompareDiffException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        }

        List<TargetDB> dbs = result.getCalculationResult();
        Assert.assertEquals(1, dbs.size());
        Assert.assertEquals("NSEARCH_GROUP_2", dbs.get(0).getDbIndex());
        Assert.assertEquals(1, dbs.get(0).getTableNames().size());
        Assert.assertEquals("nserch_1", dbs.get(0).getTableNames().iterator().next());
    }
}
