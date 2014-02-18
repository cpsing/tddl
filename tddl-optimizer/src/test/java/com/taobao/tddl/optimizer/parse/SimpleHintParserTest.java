package com.taobao.tddl.optimizer.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.taobao.tddl.optimizer.parse.hint.DirectlyRouteCondition;
import com.taobao.tddl.optimizer.parse.hint.ExtraCmdRouteCondition;
import com.taobao.tddl.optimizer.parse.hint.RouteCondition;
import com.taobao.tddl.optimizer.parse.hint.RuleRouteCondition;
import com.taobao.tddl.optimizer.parse.hint.SimpleHintParser;

public class SimpleHintParserTest {

    @Test
    public void testParser() {
        Map<String, Object> map1 = new HashMap<String, Object>();
        map1.put("relation", "and");
        map1.put("paramtype", "int");
        List<String> exprs = new ArrayList<String>();
        exprs.add("pk>4");
        exprs.add("pk<10");
        map1.put("expr", JSON.toJSON(exprs));
        List<Object> paramsO = new ArrayList<Object>();
        paramsO.add(JSON.toJSON(map1));
        Map<String, Object> hintmap = new HashMap<String, Object>();
        hintmap.put("params", JSON.toJSON(paramsO));
        hintmap.put("type", "condition");
        hintmap.put("vtab", "vtabxxx");
        String text = JSON.toJSONString(hintmap);
        System.out.println(text);

        RouteCondition route = SimpleHintParser.convertHint2RouteCondition("/*+TDDL(" + text + ")*/", null);
        System.out.println(route);
    }

    @Test
    public void testDirect() {
        String sql = "/*+TDDL({\"type\":\"direct\",\"vtab\":\"real_tab\",\"dbid\":\"xxx_group\",\"realtabs\":[\"real_tab_0\",\"real_tab_1\"]})*/select * from real_tab";
        DirectlyRouteCondition route = (DirectlyRouteCondition) SimpleHintParser.convertHint2RouteCondition(sql, null);
        System.out.println(route);
        Assert.assertEquals("xxx_group", route.getDbId());
        Assert.assertEquals(2, route.getTables().size());
        Assert.assertEquals("real_tab", route.getVirtualTableName());
    }

    @Test
    public void testCondition() {
        String sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"vtabxxx\",\"params\":[{\"relation\":\"and\",\"expr\":[\"pk>4\",\"pk<10\"],\"paramtype\":\"int\"}],\"skip\":10,\"max\":20,\"orderby\":\"col1\",\"asc\":\"true\"})*/";
        RuleRouteCondition route = (RuleRouteCondition) SimpleHintParser.convertHint2RouteCondition(sql, null);
        System.out.println(route);
        Assert.assertEquals("vtabxxx", route.getVirtualTableName());
        Assert.assertEquals("(>4) AND (<10)", route.getParameters().get("PK").toString());
    }

    @Test
    public void testExtraCmd() {
        String sql = "/*+TDDL({\"extra\":{\"ChooseIndex\":\"true\",\"ALLOW_TEMPORARY_TABLE\":\"true\"}})*/select * from real_tab";
        ExtraCmdRouteCondition route = (ExtraCmdRouteCondition) SimpleHintParser.convertHint2RouteCondition(sql, null);
        System.out.println(route);
        Assert.assertEquals("{CHOOSEINDEX=true, ALLOW_TEMPORARY_TABLE=true}", route.getExtraCmds().toString());
    }
}
