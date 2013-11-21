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
public class TableKVIndexStat {

    // 表名
    private String            tablename;
    // 所有索引采集的元数据
    private List<KVIndexStat> listKvIndexMeta;

    public TableKVIndexStat(String tablename){
        this.tablename = tablename;
        this.listKvIndexMeta = new LinkedList<KVIndexStat>();
    }

    public void addKVIndexMeta(KVIndexStat kvIndexMeta) {
        if (kvIndexMeta == null) {
            return;
        }
        listKvIndexMeta.add(kvIndexMeta);
    }

    public String getTablename() {
        return tablename;
    }

    public List<KVIndexStat> getListKvIndexMeta() {
        return listKvIndexMeta;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
