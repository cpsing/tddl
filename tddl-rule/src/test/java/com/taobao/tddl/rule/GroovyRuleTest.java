package com.taobao.tddl.rule;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.rule.impl.GroovyRule;
import com.taobao.tddl.rule.model.MatcherResult;
import com.taobao.tddl.rule.model.TargetDB;
import com.taobao.tddl.rule.model.sqljep.Comparative;

public class GroovyRuleTest extends BaseRuleTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Test
    public void test_equal() throws TddlException {
        Rule<String> dbRule = new GroovyRule<String>("\"db\"+(#id,1,64# % 64).intdiv(4)");
        Rule<String> tbRule = new GroovyRule<String>("String.valueOf(#id,1,64# % 64)");
        TableRule vt = new TableRule();
        List<Rule<String>> dbRules = new ArrayList<Rule<String>>();
        dbRules.add(dbRule);
        vt.setDbShardRules(dbRules);
        List<Rule<String>> tbRules = new ArrayList<Rule<String>>();
        tbRules.add(tbRule);
        vt.setTbShardRules(tbRules);
        vt.init();

        Choicer choicer = new Choicer();
        choicer.addComparative("ID", new Comparative(Comparative.Equivalent, 18));
        VirtualTableRuleMatcher vtrm = new VirtualTableRuleMatcher();
        MatcherResult mr = vtrm.match(choicer, Lists.newArrayList(), vt, false);
        List<TargetDB> targetDb = mr.getCalculationResult();
        Assert.assertEquals(1, targetDb.size());
        Assert.assertEquals(1, targetDb.get(0).getTableNames().size());
        Assert.assertEquals("db4", targetDb.get(0).getDbIndex());
        Assert.assertEquals("18", targetDb.get(0).getTableNames().iterator().next());
    }

    @Test
    public void testUpdateIn() throws TddlException {
        Rule<String> dbRule = new GroovyRule<String>("\"db\"+(#id,1,64# % 64).intdiv(4)");
        Rule<String> tbRule = new GroovyRule<String>("String.valueOf(#id,1,64# % 64)");
        TableRule vt = new TableRule();
        List<Rule<String>> dbRules = new ArrayList<Rule<String>>();
        dbRules.add(dbRule);
        vt.setDbShardRules(dbRules);
        List<Rule<String>> tbRules = new ArrayList<Rule<String>>();
        tbRules.add(tbRule);
        vt.setTbShardRules(tbRules);
        vt.init();
        Choicer choicer = new Choicer();
        choicer.addComparative("ID", or(0, 4, 18, 19, 64));
        VirtualTableRuleMatcher vtrm = new VirtualTableRuleMatcher();
        MatcherResult mr = vtrm.match(choicer, Lists.newArrayList(), vt, false);
        List<TargetDB> targetDb = mr.getCalculationResult();
        Assert.assertEquals(3, targetDb.size());
    }

    @Test
    public void testRange() throws ParseException, TddlException {
        Rule<String> dbRule = new GroovyRule<String>("\"db\"+(getCalendar(#time,1_month,2#).get(Calendar.MONTH)%2)");
        Rule<String> tbRule = new GroovyRule<String>("\"\"+getCalendar(#time,1_date,365#).get(Calendar.DATE)");
        TableRule vt = new TableRule();
        List<Rule<String>> dbRules = new ArrayList<Rule<String>>();
        dbRules.add(dbRule);
        vt.setDbShardRules(dbRules);
        List<Rule<String>> tbRules = new ArrayList<Rule<String>>();
        tbRules.add(tbRule);
        vt.setTbShardRules(tbRules);
        vt.init();
        Choicer choicer = new Choicer();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Comparative d1 = new Comparative(Comparative.GreaterThan, df.parse("2010-10-29"));
        Comparative d2 = new Comparative(Comparative.LessThan, df.parse("2010-11-03"));
        choicer.addComparative("TIME", and(d1, d2));
        VirtualTableRuleMatcher vtrm = new VirtualTableRuleMatcher();
        MatcherResult mr = vtrm.match(choicer, Arrays.asList(new Object[] { 18 }), vt, false);
        List<TargetDB> targetDb = mr.getCalculationResult();
        Assert.assertEquals(2, targetDb.size());
    }

    @Test
    public void testNull() throws ParseException, TddlException {
        Rule<String> dbRule = new GroovyRule<String>("\"db\"+(#id,1,64#.toString())");
        Rule<String> tbRule = new GroovyRule<String>("#id,1,64#.toString()");
        TableRule vt = new TableRule();
        List<Rule<String>> dbRules = new ArrayList<Rule<String>>();
        dbRules.add(dbRule);
        vt.setDbShardRules(dbRules);
        List<Rule<String>> tbRules = new ArrayList<Rule<String>>();
        tbRules.add(tbRule);
        vt.setTbShardRules(tbRules);
        vt.init();

        // null对象的toString()方法计算结果为null
        Choicer choicer = new Choicer();
        choicer.addComparative("ID", new Comparative(Comparative.Equivalent, null));
        VirtualTableRuleMatcher vtrm = new VirtualTableRuleMatcher();
        MatcherResult mr = vtrm.match(choicer, Lists.newArrayList(), vt, false);
        List<TargetDB> targetDb = mr.getCalculationResult();
        Assert.assertEquals(1, targetDb.size());
        Assert.assertEquals(1, targetDb.get(0).getTableNames().size());
        Assert.assertEquals("dbnull", targetDb.get(0).getDbIndex());
        Assert.assertEquals("null", targetDb.get(0).getTableNames().iterator().next());
    }

    @Test
    public void testDate() throws ParseException, TddlException {
        Rule<String> dbRule = new GroovyRule<String>("\"db\"+(#id,1,64#.toString())");
        Rule<String> tbRule = new GroovyRule<String>("(getCalendar(#gmt_create,1_date,31#).get(Calendar.DAY_OF_MONTH) % 32) - 1");
        TableRule vt = new TableRule();
        List<Rule<String>> dbRules = new ArrayList<Rule<String>>();
        dbRules.add(dbRule);
        vt.setDbShardRules(dbRules);
        List<Rule<String>> tbRules = new ArrayList<Rule<String>>();
        tbRules.add(tbRule);
        vt.setTbShardRules(tbRules);
        vt.init();
        Choicer choicer = new Choicer();
        choicer.addComparative("ID", new Comparative(Comparative.Equivalent, null));
        choicer.addComparative("GMT_CREATE", new Comparative(Comparative.Equivalent, getDate(2013, 1, 32, 12, 12, 12)));
        VirtualTableRuleMatcher vtrm = new VirtualTableRuleMatcher();
        MatcherResult mr = vtrm.match(choicer, Lists.newArrayList(), vt, false);
        List<TargetDB> targetDb = mr.getCalculationResult();
        System.out.println(targetDb);
    }

    @SuppressWarnings("deprecation")
    public Date getDate(int year, int month, int date, int hrs, int min, int sec) {
        return new Date(year, month, date, hrs, min, sec);
    }
}
