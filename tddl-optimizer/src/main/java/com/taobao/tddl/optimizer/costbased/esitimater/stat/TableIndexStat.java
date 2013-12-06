package com.taobao.tddl.optimizer.costbased.esitimater.stat;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 一个表的所有索引的统计数据
 * 
 * @author danchen
 */
public class TableIndexStat {

    // 表名
    private String            tableName;
    // 所有索引采集的元数据
    private List<KVIndexStat> indexStats = new LinkedList<KVIndexStat>();

    public void addKVIndexStat(KVIndexStat kvIndexStat) {
        if (kvIndexStat == null) {
            return;
        }
        indexStats.add(kvIndexStat);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<KVIndexStat> getIndexStats() {
        return indexStats;
    }

    public void setIndexStats(List<KVIndexStat> indexStats) {
        this.indexStats = indexStats;
    }

    public KVIndexStat getIndexStat(String indexName) {
        for (KVIndexStat indexStat : indexStats) {
            if (indexName.equals(indexStat.getIndexName())) {
                return indexStat;
            }
        }

        return null;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
