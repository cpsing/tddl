package com.taobao.tddl.rule.model.sqljep;

/**
 * <pre>
 * AND节点 ,在实际的SQL中，实际上是类似
 * [Comparative]              [comparative]
 *          \                  /
 *            \               /
 *             [ComparativeAnd]
 * 类似这样的节点出现
 * </pre>
 * 
 * @author shenxun
 */
public class ComparativeAND extends ComparativeBaseList {

    public ComparativeAND(int function, Comparable<?> value){
        super(function, value);
    }

    public ComparativeAND(){
    }

    public ComparativeAND(Comparative item){
        super(item);
    }

    protected String getRelation() {
        return " AND ";
    }

}
