package com.taobao.tddl.optimizer.core.expression.bean;

/**
 * @author jianghang 2013-11-15 下午2:06:58
 * @since 5.1.0
 */
public class LobVal implements Comparable<String> {

    private String str;
    private String introducer;

    public LobVal(String str, String introducer){
        this.str = str;
        this.introducer = introducer;
    }

    @Override
    public int compareTo(String o) {
        return this.str.compareTo(o);
    }

    public String getStr() {
        return str;
    }

    public String getIntroducer() {
        return introducer;
    }
}
