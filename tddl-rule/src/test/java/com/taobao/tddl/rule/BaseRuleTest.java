package com.taobao.tddl.rule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;

import com.taobao.tddl.rule.model.sqljep.Comparative;
import com.taobao.tddl.rule.model.sqljep.ComparativeAND;
import com.taobao.tddl.rule.model.sqljep.ComparativeMapChoicer;
import com.taobao.tddl.rule.model.sqljep.ComparativeOR;

@Ignore
public class BaseRuleTest {

    protected Comparative or(Comparable... values) {
        ComparativeOR and = new ComparativeOR();
        for (Comparable obj : values) {
            and.addComparative(new Comparative(Comparative.Equivalent, obj));
        }
        return and;
    }

    protected Comparative and(Comparative... values) {
        ComparativeAND and = new ComparativeAND();
        for (Comparative obj : values) {
            and.addComparative(obj);
        }
        return and;
    }

    protected static class Choicer implements ComparativeMapChoicer {

        private Map<String, Comparative> comparatives = new HashMap<String, Comparative>();

        public Choicer(){

        }

        public Choicer(Map<String, Comparative> comparatives){
            this.comparatives = comparatives;
        }

        public void addComparative(String name, Comparative comparative) {
            this.comparatives.put(name, comparative);
        }

        public Comparative getColumnComparative(List<Object> arguments, String colName) {
            return comparatives.get(colName);
        }

        public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> partnationSet) {
            return null;
        }
    }
}
