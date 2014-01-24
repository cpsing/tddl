package com.taobao.tddl.rule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.taobao.tddl.rule.model.sqljep.Comparative;
import com.taobao.tddl.rule.model.sqljep.ComparativeAND;
import com.taobao.tddl.rule.model.sqljep.ComparativeOR;

public class TestUtils {

    public static final int GreaterThan        = Comparative.GreaterThan;
    public static final int GreaterThanOrEqual = Comparative.GreaterThanOrEqual;
    public static final int Equivalent         = Comparative.Equivalent;
    public static final int NotEquivalent      = Comparative.NotEquivalent;
    public static final int LessThan           = Comparative.LessThan;
    public static final int LessThanOrEqual    = Comparative.LessThanOrEqual;

    public static Comparative gor(Comparative parent, Comparative target) {
        if (parent == null) {
            ComparativeOR or = new ComparativeOR();
            or.addComparative(target);
            return or;
        } else {
            if (parent instanceof ComparativeOR) {
                ((ComparativeOR) parent).addComparative(target);
                return parent;
            } else {
                ComparativeOR or = new ComparativeOR();
                or.addComparative(parent);
                or.addComparative(target);
                return or;
            }
        }
    }

    public static Comparative gand(Comparative parent, Comparative target) {
        if (parent == null) {

            ComparativeAND and = new ComparativeAND();
            and.addComparative(target);
            return and;
        } else {
            if (parent instanceof ComparativeAND) {

                ComparativeAND and = ((ComparativeAND) parent);
                if (and.getList().size() == 1) {
                    and.addComparative(target);
                    return and;
                } else {
                    ComparativeAND andNew = new ComparativeAND();
                    andNew.addComparative(and);
                    andNew.addComparative(target);
                    return andNew;
                }

            } else {
                ComparativeAND and = new ComparativeAND();
                and.addComparative(parent);
                and.addComparative(target);
                return and;
            }
        }
    }

    public static Comparative gcomp(Comparable comp, int sym) {
        return new Comparative(sym, comp);
    }

    public static void testSet(Object[] target, Set<? extends Object> beTestedSet) {
        assertEquals(target.length, beTestedSet.size());
        int index = 0;

        for (Object obj : target) {
            assertTrue("index:" + String.valueOf(index) + "-value:" + obj + "|set:" + beTestedSet,
                beTestedSet.contains(obj));
            index++;
        }
    }

    public static void testSetDate(Date[] target, Set<Object> beTestedSet) {
        assertEquals(target.length, beTestedSet.size());
        Set<String> dateStr = new HashSet<String>();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
        for (Object date : beTestedSet) {
            String formated = format.format(((Date) date));
            dateStr.add(formated);
        }
        Set<String> targetDateString = new HashSet<String>();
        for (Date date : target) {
            String formated = format.format(((Date) date));
            targetDateString.add(formated);
        }
        int index = 0;

        Iterator<String> strs = dateStr.iterator();
        while (strs.hasNext()) {

            boolean isTrue = false;
            StringBuilder sb = new StringBuilder();

            String str = strs.next();
            sb.append(str).append("|");
            for (String obj : targetDateString) {

                if (str.trim().equals(obj.trim())) {
                    strs.remove();
                    isTrue = true;
                }
            }

            assertTrue("index:" + String.valueOf(index) + "-value:" + targetDateString + " target:" + sb.toString(),
                isTrue);
            index++;

        }
    }
}
