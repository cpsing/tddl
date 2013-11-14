package com.taobao.tddl.optimizer.config.table;

/**
 * Defines the relationship between instances of the entity class and the
 * secondary keys.
 * 
 * @author jianxing <jianxing.qx@taobao.com>
 */
public interface Relationship {

    int NONE         = 0;
    int ONE_TO_ONE   = 3;
    int MANY_TO_ONE  = 4;
    int ONE_TO_MANY  = 5;
    int MANY_TO_MANY = 6;

}
