package com.taobao.tddl.rule.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.common.model.sqljep.Comparative;
import com.taobao.tddl.common.model.sqljep.ComparativeAND;
import com.taobao.tddl.common.model.sqljep.ComparativeBaseList;
import com.taobao.tddl.common.model.sqljep.ComparativeOR;
import com.taobao.tddl.common.utils.TStringUtil;

public class ComparativeStringAnalyser {

    public static Map<String, Comparative> decodeComparativeString2Map(String conditionStr) {
        Map<String, Comparative> comparativeMap = new HashMap<String, Comparative>();
        // 处理 ComparativeMap
        String[] comStrs = conditionStr.split(";");
        for (int i = 0; i < comStrs.length; i++) {
            String value = comStrs[i].toLowerCase();
            if (TStringUtil.isNotBlank(value)) {
                boolean containsAnd = TStringUtil.contains(value, " and ");
                boolean containsOr = TStringUtil.contains(value, " or ");
                if (containsAnd || containsOr) {
                    ComparativeBaseList comparativeBaseList = null;
                    String op;
                    if (containsOr) {
                        comparativeBaseList = new ComparativeOR();
                        op = "or";
                    } else if (containsAnd) {
                        comparativeBaseList = new ComparativeAND();
                        op = "and";
                    } else {
                        throw new RuntimeException("decodeComparative not support ComparativeBaseList value:" + value);
                    }
                    String[] compValues = twoPartSplit(value, op);
                    String key = null;
                    for (String compValue : compValues) {
                        Comparative comparative = decodeComparativeForOuter(compValue);
                        if (null != comparative) {
                            comparativeBaseList.addComparative(comparative);
                        }
                        String temp = getComparativeKey(compValue).trim();
                        if (null == key) {
                            key = temp;
                        } else if (!temp.equals(key)) {
                            throw new RuntimeException("decodeComparative not support ComparativeBaseList value:"
                                                       + value);
                        }
                    }
                    comparativeMap.put(key.toUpperCase(), comparativeBaseList);
                } else {
                    // 说明只是Comparative
                    String key = getComparativeKey(value);
                    Comparative comparative = decodeComparativeForOuter(value);
                    if (null != comparative) {
                        comparativeMap.put(key.toUpperCase().trim(), comparative);
                    }
                }
            }
        }
        return comparativeMap;
    }

    protected static Comparative decodeComparativeForOuter(String compValue) {
        boolean containsIn = TStringUtil.contains(compValue, " in");
        Comparative comparative = null;
        if (!containsIn) {
            int compEnum = Comparative.getComparisonByCompleteString(compValue);
            String splitor = Comparative.getComparisonName(compEnum);
            int size = splitor.length();
            int index = compValue.indexOf(splitor);
            String valueTypeStr = TStringUtil.substring(compValue, index + size);
            int lastColonIndex = valueTypeStr.lastIndexOf(":");
            String value = valueTypeStr.substring(0, lastColonIndex);
            String type = valueTypeStr.substring(lastColonIndex + 1);
            if (null != type && null != value) {
                if ("i".equals(type.trim())) {
                    comparative = new Comparative(compEnum, Integer.valueOf(value.trim()));
                } else if ("l".equals(type.trim())) {
                    comparative = new Comparative(compEnum, Long.valueOf(value.trim()));
                } else if ("s".equals(type.trim())) {
                    comparative = new Comparative(compEnum, value.trim());
                } else if ("d".equals(type.trim())) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    try {
                        comparative = new Comparative(compEnum, sdf.parse(value.trim()));
                    } catch (ParseException e) {
                        throw new RuntimeException("only support 'yyyy-MM-dd',now date string is:" + value.trim());
                    }
                } else if ("int".equals(type.trim())) {
                    comparative = new Comparative(compEnum, Integer.valueOf(value.trim()));
                } else if ("long".equals(type.trim())) {
                    comparative = new Comparative(compEnum, Long.valueOf(value.trim()));
                } else if ("string".equals(type.trim())) {
                    comparative = new Comparative(compEnum, value.trim());
                } else if ("date".equals(type.trim())) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    try {
                        comparative = new Comparative(compEnum, sdf.parse(value.trim()));
                    } catch (ParseException e) {
                        throw new RuntimeException("only support 'yyyy-MM-dd',now date string is:" + value.trim());
                    }
                } else {
                    throw new RuntimeException("decodeComparative Error notSupport Comparative valueType value: "
                                               + compValue);
                }
            } else {
                throw new RuntimeException("decodeComparative Error notSupport Comparative valueType value: "
                                           + compValue);
            }
        } else {
            String[] compValues = twoPartSplit(compValue, " in");
            int lastColonIndex = compValues[1].lastIndexOf(":");
            String inValues = compValues[1].substring(0, lastColonIndex);
            String type = compValues[1].substring(lastColonIndex + 1);
            if (null != inValues && null != type) {
                ComparativeOR comparativeBaseList = new ComparativeOR();
                String[] values = TStringUtil.split(getBetween(inValues, "(", ")"), ",");
                if ("i".equals(type.trim())) {
                    for (String value : values) {
                        Comparative temp = new Comparative(Comparative.Equivalent, Integer.valueOf(value.trim()));
                        comparativeBaseList.addComparative(temp);
                    }
                } else if ("l".equals(type.trim())) {
                    for (String value : values) {
                        Comparative temp = new Comparative(Comparative.Equivalent, Long.valueOf(value.trim()));
                        comparativeBaseList.addComparative(temp);
                    }
                } else if ("s".equals(type.trim())) {
                    for (String value : values) {
                        Comparative temp = new Comparative(Comparative.Equivalent, value.trim());
                        comparativeBaseList.addComparative(temp);
                    }
                } else if ("d".equals(type.trim())) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    for (String value : values) {
                        Comparative temp = null;
                        try {
                            temp = new Comparative(Comparative.Equivalent, sdf.parse(value.trim()));
                        } catch (ParseException e) {
                            throw new RuntimeException("only support 'yyyy-MM-dd',now date string is:" + value.trim());
                        }
                        comparativeBaseList.addComparative(temp);
                    }
                } else if ("int".equals(type.trim())) {
                    for (String value : values) {
                        Comparative temp = new Comparative(Comparative.Equivalent, Integer.valueOf(value.trim()));
                        comparativeBaseList.addComparative(temp);
                    }
                } else if ("long".equals(type.trim())) {
                    for (String value : values) {
                        Comparative temp = new Comparative(Comparative.Equivalent, Long.valueOf(value.trim()));
                        comparativeBaseList.addComparative(temp);
                    }
                } else if ("string".equals(type.trim())) {
                    for (String value : values) {
                        Comparative temp = new Comparative(Comparative.Equivalent, value.trim());
                        comparativeBaseList.addComparative(temp);
                    }
                } else if ("date".equals(type.trim())) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    for (String value : values) {
                        Comparative temp = null;
                        try {
                            temp = new Comparative(Comparative.Equivalent, sdf.parse(value.trim()));
                        } catch (ParseException e) {
                            throw new RuntimeException("only support 'yyyy-MM-dd',now date string is:" + value.trim());
                        }
                        comparativeBaseList.addComparative(temp);
                    }
                } else {
                    throw new RuntimeException("decodeComparative Error notSupport Comparative valueType value: "
                                               + compValue);
                }
                comparative = comparativeBaseList;
            }
        }

        return comparative;
    }

    /**
     * 只做一次切分
     * 
     * @param str
     * @param splitor
     * @return
     */
    private static String[] twoPartSplit(String str, String splitor) {
        if (splitor != null) {
            int index = str.indexOf(splitor);
            String first = str.substring(0, index);
            String sec = str.substring(index + splitor.length());
            return new String[] { first, sec };
        } else {
            return new String[] { str };
        }
    }

    private static String getComparativeKey(String compValue) {
        boolean containsIn = TStringUtil.contains(compValue, " in");
        if (containsIn) {
            String[] compValues = twoPartSplit(compValue, " in");
            return compValues[0];
        } else {
            int value = Comparative.getComparisonByCompleteString(compValue);
            String splitor = Comparative.getComparisonName(value);
            int index = compValue.indexOf(splitor);
            return TStringUtil.substring(compValue, 0, index);
        }
    }

    /**
     * 获得第一个start，end之间的字串， 不包括start，end本身。返回值已做了trim
     */
    private static String getBetween(String sql, String start, String end) {
        int index0 = sql.indexOf(start);
        if (index0 == -1) {
            return null;
        }
        int index1 = sql.indexOf(end, index0);
        if (index1 == -1) {
            return null;
        }
        return sql.substring(index0 + start.length(), index1).trim();
    }
}
