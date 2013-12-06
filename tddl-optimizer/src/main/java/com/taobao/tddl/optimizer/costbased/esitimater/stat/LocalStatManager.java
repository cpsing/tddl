package com.taobao.tddl.optimizer.costbased.esitimater.stat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.TddlToStringStyle;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.parse.TableIndexStatParser;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.parse.TableStatParser;

/**
 * 本地文件的schema manager实现
 * 
 * @since 5.1.0
 */
public class LocalStatManager extends AbstractLifecycle implements StatManager {

    protected Map<String, KVIndexStat> kvIndexCache;
    protected Map<String, TableStat>   tableCache;

    protected void doInit() throws TddlException {
        super.doInit();
        kvIndexCache = new ConcurrentHashMap<String, KVIndexStat>();
        tableCache = new ConcurrentHashMap<String, TableStat>();
    }

    protected void doDestory() throws TddlException {
        super.doDestory();
        kvIndexCache.clear();
        tableCache.clear();
    }

    public KVIndexStat getKVIndex(String indexName) {
        return kvIndexCache.get(indexName);
    }

    public TableStat getTable(String tableName) {
        return tableCache.get(tableName);
    }

    public void putKVIndex(String indexName, KVIndexStat stat) {
        this.kvIndexCache.put(indexName, stat);
    }

    public void putTable(String tableName, TableStat stat) {
        this.tableCache.put(tableName, stat);
    }

    public static LocalStatManager parseConfig(String table, String index) throws TddlException {
        if (table == null || table.isEmpty()) {
            throw new IllegalArgumentException("table is null");
        }

        if (index == null || index.isEmpty()) {
            throw new IllegalArgumentException("index is null");
        }

        InputStream tableIn = null;
        InputStream indexIn = null;
        try {
            tableIn = new ByteArrayInputStream(table.getBytes());
            indexIn = new ByteArrayInputStream(index.getBytes());
            return parseConfig(tableIn, indexIn);
        } finally {
            IOUtils.closeQuietly(tableIn);
            IOUtils.closeQuietly(indexIn);
        }
    }

    public static LocalStatManager parseConfig(InputStream table, InputStream index) throws TddlException {
        if (table == null) {
            throw new IllegalArgumentException("table stream is null");
        }

        if (index == null) {
            throw new IllegalArgumentException("index stream is null");
        }

        try {
            LocalStatManager statManager = new LocalStatManager();
            List<TableStat> tableStatList = TableStatParser.parse(table);
            for (TableStat t : tableStatList) {
                statManager.putTable(t.getTableName(), t);
            }

            List<TableIndexStat> tableIndexStatList = TableIndexStatParser.parse(index);
            for (TableIndexStat t : tableIndexStatList) {
                for (KVIndexStat indexStat : t.getIndexStats()) {
                    statManager.putKVIndex(indexStat.getIndexName(), indexStat);
                }
            }

            return statManager;
        } finally {
            IOUtils.closeQuietly(index);
            IOUtils.closeQuietly(table);
        }

    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
