package com.taobao.tddl.rule.app;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.tddl.rule.TddlRule;
import com.taobao.tddl.rule.model.MatcherResult;
import com.taobao.tddl.rule.model.TargetDB;

public class EagleEyeRuleTest {

    static TddlRule rule;

    @BeforeClass
    public static void setUp() {
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:app/eagleEye/spring-context.xml");
        rule = (TddlRule) context.getBean("rule");
    }

    @Test
    public void testEagleEye() {
        MatcherResult target = rule.route("hsflog", "days=1:int");
        for (TargetDB db : target.getCalculationResult()) {
            System.out.println("--------------------------");
            System.out.println(db.getDbIndex() + " ---------> " + db.getTableNames());
        }

        target = rule.route("hsflog", "traceid=1:int");
        for (TargetDB db : target.getCalculationResult()) {
            System.out.println("--------------------------");
            System.out.println(db.getDbIndex() + " ---------> " + db.getTableNames());
        }
    }

    @Test
    public void testNoitfy() {
        MatcherResult target = rule.route("notify_msg", "message_id=1:int");
        for (TargetDB db : target.getCalculationResult()) {
            System.out.println("--------------------------");
            System.out.println(db.getDbIndex() + " ---------> " + db.getTableNames());
        }

        target = rule.route("notify_msg", "gmt_create_days=1:int");
        for (TargetDB db : target.getCalculationResult()) {
            System.out.println("--------------------------");
            System.out.println(db.getDbIndex() + " ---------> " + db.getTableNames());
        }
    }
}
