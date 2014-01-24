package com.taobao.tddl.rule.model.sqljep;

/**
 * <pre>
 * OR节点, 在实际的SQL中，实际上是类似
 * [Comparative]              [comparative]
 *          \                  /
 *            \               /
 *             [ComparativeOR]
 *             
 * 类似这样的节点出现
 * </pre>
 * 
 * @author shenxun
 */
public class ComparativeOR extends ComparativeBaseList {

    public ComparativeOR(int function, Comparable<?> value){
        super(function, value);
    }

    public ComparativeOR(){
    }

    public ComparativeOR(Comparative item){
        super(item);
    }

    public ComparativeOR(int capacity){
        super(capacity);
    }

    protected String getRelation() {
        return " OR ";
    }
}
