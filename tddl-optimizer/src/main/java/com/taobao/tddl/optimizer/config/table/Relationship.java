package com.taobao.tddl.optimizer.config.table;

/**
 * Defines the relationship between instances of the entity class and the
 * secondary keys.
 * 
 * @author jianxing <jianxing.qx@taobao.com>
 */
public enum Relationship {
    NONE, ONE_TO_ONE, MANY_TO_ONE, ONE_TO_MANY, MANY_TO_MANY;
}
