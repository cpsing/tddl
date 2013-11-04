package com.taobao.tddl.rule.impl;

import java.util.Map;

public class WrappedGroovyRule extends GroovyRule<String> {

    protected String valuePrefix;      // 无getter/setter
    protected String valueSuffix;      // 无getter/setter
    protected int    valueAlignLen = 0; // 无getter/setter

    public WrappedGroovyRule(String expression, String wrapPattern){
        super(expression);
        setValueWrappingPattern(wrapPattern);
    }

    public WrappedGroovyRule(String expression, String wrapPattern, String extraPackagesStr){
        super(expression, extraPackagesStr);
        setValueWrappingPattern(wrapPattern);
    }

    private void setValueWrappingPattern(String wrapPattern) {
        int index0 = wrapPattern.indexOf('{');
        if (index0 == -1) {
            this.valuePrefix = wrapPattern;
            return;
        }
        int index1 = wrapPattern.indexOf('}', index0);
        if (index1 == -1) {
            this.valuePrefix = wrapPattern;
            return;
        }
        this.valuePrefix = wrapPattern.substring(0, index0);
        this.valueSuffix = wrapPattern.substring(index1 + 1);
        this.valueAlignLen = index1 - index0 - 1; // {0000}中0的个数
    }

    protected static String wrapValue(String prefix, String suffix, int len, String value) {
        StringBuilder sb = new StringBuilder();
        if (prefix != null) {
            sb.append(prefix);
        }
        if (len > 1) {
            int k = len - value.length();
            for (int i = 0; i < k; i++) {
                sb.append("0");
            }
        }
        sb.append(value);
        if (suffix != null) {
            sb.append(suffix);
        }
        return sb.toString();
    }

    @Override
    public String eval(Map<String, Object> columnValues, Object outerContext) {
        String value = super.eval(columnValues, outerContext);
        return wrapValue(valuePrefix, valueSuffix, valueAlignLen, value);
    }
}
