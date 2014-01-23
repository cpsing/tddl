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
import com.taobao.tddl.rule.exceptions.RouteCompareDiffException;
import com.taobao.tddl.rule.model.MatcherResult;
import com.taobao.tddl.rule.model.TargetDB;
import com.taobao.tddl.rule.utils.ComparativeStringAnalyser;

public class RemoteRuleIntegration extends BaseRuleTest {

    static TddlRule rule;

    @BeforeClass
    public static void setUp() {
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:remote/spring-context.xml");
        rule = (TddlRule) context.getBean("rule");
    }

    @AfterClass
    public static void tearDown() throws TddlException {
        rule.destory();
    }

    @Test
    public void testTddlRuleMuRulesAndCompare_Select() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String conditionStr = "message_id >=25:int and message_id<=26:int;gmt_create>=" + sdf.format(new Date())
                              + ":date";

        try {
            List<TargetDB> db = null;
            MatcherResult result = rule.routeMverAndCompare(true,
                "nserch",
                new Choicer(ComparativeStringAnalyser.decodeComparativeString2Map(conditionStr)),
                Lists.newArrayList());
            db = result.getCalculationResult();
            for (TargetDB targetDatabase : db) {
                StringBuilder sb = new StringBuilder("目标库:");
                sb.append(targetDatabase.getDbIndex());
                sb.append(" 所要执行的表:");
                for (String table : targetDatabase.getTableNames()) {
                    sb.append(table);
                    sb.append(" ");
                }
                System.out.println(sb.toString());
            }
        } catch (RouteCompareDiffException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        }

    }

    @Test
    public void testTddlRuleMuRulesAndCompare_Insert() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String conditionStr = "message_id >=25:int and message_id<=26:int;gmt_create>=" + sdf.format(new Date())
                              + ":date";

        try {
            List<TargetDB> db = null;
            MatcherResult result = rule.routeMverAndCompare(true,
                "nserch",
                new Choicer(ComparativeStringAnalyser.decodeComparativeString2Map(conditionStr)),
                Lists.newArrayList());
            db = result.getCalculationResult();
            for (TargetDB targetDatabase : db) {
                StringBuilder sb = new StringBuilder("目标库:");
                sb.append(targetDatabase.getDbIndex());
                sb.append(" 所要执行的表:");
                for (String table : targetDatabase.getTableNames()) {
                    sb.append(table);
                    sb.append(" ");
                }
                System.out.println(sb.toString());
            }
        } catch (RouteCompareDiffException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        }
    }
}
