package com.taobao.tddl.optimizer.costbased.esitimater.stat;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 单个索引的统计数据
 * 
 * @author danchen
 */
public class KVIndexStat {

    // 日志
    private final static Log logger = LogFactory.getLog(KVIndexStat.class);
    // 索引的名字
    private String           indexname;
    // 索引的类型
    private int              indextype;
    // 不同值的个数
    private int              distinct_keys;
    // 采样大小
    private int              num_rows;
    // 索引的选择度,至少大于0
    // 0<=factor<=1 越接近1,索引的选择度越大
    // 如果factor很接近为0,并不一定代表着索引的选择度就低,这时候列的柱状图将发挥作用
    private double           factor;

    public KVIndexStat(String indexname, int indextype){
        this.indexname = indexname;
        this.indextype = indextype;
        this.factor = 0;
        this.distinct_keys = 0;
        this.num_rows = 0;
    }

    public String getIndexname() {
        return indexname;
    }

    public int getIndextype() {
        return indextype;
    }

    public int getDistinct_keys() {
        return distinct_keys;
    }

    public int getNum_rows() {
        return num_rows;
    }

    public double getKVIndexFactor() {
        return factor;
    }

    public void setKVIndexFactor(double factor) {
        this.factor = factor;
    }

    /**
     * 统计数据的计算公式 factor=distinct_keys/num_rows;
     */
    public void computeCard(int realsampleRows, int countDisttinctKeys) {

        if (realsampleRows > 0 && countDisttinctKeys > 0 && realsampleRows >= countDisttinctKeys) {
            this.distinct_keys = countDisttinctKeys;
            this.num_rows = realsampleRows;
            this.factor = (double) distinct_keys / (double) num_rows;
        } else {
            this.factor = 0;
            logger.debug("parameter check result:countDisttinctKeys=0");
        }

    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
