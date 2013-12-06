package com.taobao.tddl.optimizer.costbased.esitimater.stat;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 单个索引的统计数据
 * 
 * @author danchen
 */
public class KVIndexStat {

    // 日志
    private static final Logger logger = LoggerFactory.getLogger(KVIndexStat.class);
    // 索引的名字
    private String              indexName;
    // 索引的类型
    private int                 indexType;
    // 不同值的个数
    private int                 distinctKeys;
    // 采样大小
    private int                 numRows;
    // 索引的选择度,至少大于0
    // 0<=factor<=1 越接近1,索引的选择度越大
    // 如果factor很接近为0,并不一定代表着索引的选择度就低,这时候列的柱状图将发挥作用
    private double              factor;

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public int getIndexType() {
        return indexType;
    }

    public void setIndexType(int indexType) {
        this.indexType = indexType;
    }

    public int getDistinctKeys() {
        return distinctKeys;
    }

    public void setDistinctKeys(int distinctKeys) {
        this.distinctKeys = distinctKeys;
    }

    public int getNumRows() {
        return numRows;
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    public double getFactor() {
        return factor;
    }

    public void setFactor(double factor) {
        this.factor = factor;
    }

    /**
     * 统计数据的计算公式 factor=distinct_keys/num_rows;
     */
    public void computeCard(int realsampleRows, int countDisttinctKeys) {
        if (realsampleRows > 0 && countDisttinctKeys > 0 && realsampleRows >= countDisttinctKeys) {
            this.distinctKeys = countDisttinctKeys;
            this.numRows = realsampleRows;
            this.factor = (double) distinctKeys / (double) numRows;
        } else {
            this.factor = 0;
            logger.debug("parameter check result:countDisttinctKeys=0");
        }

    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
