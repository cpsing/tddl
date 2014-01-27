package com.taobao.tddl.group.config;

/**
 * 描述如何选择group下的atom节点
 * 
 * @author jianghang 2013-10-31 上午11:42:53
 * @since 5.0.0
 */
public class GroupIndex {

    public final int     index;
    public final boolean failRetry;

    public GroupIndex(int index, boolean failRetry){
        this.index = index;
        this.failRetry = failRetry;
    }
}
