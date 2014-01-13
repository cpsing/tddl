package com.taobao.tddl.rule.model.sqljep;

/**
 * 可比较的类 实际上是是两个东西的结合 符号+值 例如 [> 1] , [< 1] , [= 1]
 * 
 * @author shenxun
 */
public class Comparative implements Comparable, Cloneable {

    public static final int GreaterThan        = 1;
    public static final int GreaterThanOrEqual = 2;
    public static final int Equivalent         = 3;
    public static final int NotEquivalent      = 4;
    public static final int LessThan           = 5;
    public static final int LessThanOrEqual    = 6;

    private Comparable      value;                 // 这有可能又是个Comparative，从而实质上表示一课树（比较树）
    private int             comparison;

    protected Comparative(){
    }

    public Comparative(int function, Comparable value){
        this.comparison = function;
        this.value = value;
    }

    /**
     * 表达式取反
     * 
     * @param function
     * @return
     */
    public static int reverseComparison(int function) {
        return 7 - function;
    }

    /**
     * 表达式前后位置调换的时候
     * 
     * @param function
     * @return
     */
    public static int exchangeComparison(int function) {
        if (function == GreaterThan) {
            return LessThan;
        } else if (function == GreaterThanOrEqual) {
            return LessThanOrEqual;
        } else if (function == LessThan) {
            return GreaterThan;
        }

        if (function == LessThanOrEqual) {
            return GreaterThanOrEqual;
        } else {
            return function;
        }
    }

    public Comparable getValue() {
        return value;
    }

    public void setComparison(int function) {
        this.comparison = function;
    }

    public static String getComparisonName(int function) {
        if (function == Equivalent) {
            return "=";
        } else if (function == GreaterThan) {
            return ">";
        } else if (function == GreaterThanOrEqual) {
            return ">=";
        } else if (function == LessThanOrEqual) {
            return "<=";
        } else if (function == LessThan) {
            return "<";
        } else if (function == NotEquivalent) {
            return "<>";
        } else {
            return null;
        }
    }

    /**
     * contains顺序按字符从多到少排列，否则逻辑不对，这里 先这样处理。
     * 
     * @param completeStr
     * @return
     */
    public static int getComparisonByCompleteString(String completeStr) {
        if (completeStr != null) {
            String ident = completeStr.toLowerCase();
            if (ident.contains(">=")) {
                return GreaterThanOrEqual;
            } else if (ident.contains("<=")) {
                return LessThanOrEqual;
            } else if (ident.contains("!=")) {
                return NotEquivalent;
            } else if (ident.contains("<>")) {
                return NotEquivalent;
            } else if (ident.contains("=")) {
                return Equivalent;
            } else if (ident.contains(">")) {
                return GreaterThan;
            } else if (ident.contains("<")) {
                return LessThan;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

    public int getComparison() {
        return comparison;
    }

    public void setValue(Comparable value) {
        this.value = value;
    }

    public int compareTo(Object o) {
        if (o instanceof Comparative) {
            Comparative other = (Comparative) o;
            return this.getValue().compareTo(other.getValue());
        } else if (o instanceof Comparable) {
            return this.getValue().compareTo(o);
        }
        return -1;
    }

    public String toString() {
        if (value != null) {
            StringBuilder sb = new StringBuilder();
            sb.append("(").append(getComparisonName(comparison));
            sb.append(value.toString()).append(")");
            return sb.toString();
        } else {
            return null;
        }
    }

    public Object clone() {
        return new Comparative(this.comparison, this.value);
    }
}
