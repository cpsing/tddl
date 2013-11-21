package com.taobao.tddl.optimizer.config.table;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 本地文件的schema manager实现
 * 
 * @since 5.1.0
 */
public class LocalSchemaManager extends AbstractLifecycle implements SchemaManager {

    protected Map<String, TableMeta> ss;

    protected void doInit() {
        super.doInit();
        ss = new ConcurrentHashMap<String, TableMeta>();
    }

    protected void doDestory() {
        super.doDestory();
        ss.clear();
    }

    public TableMeta getTable(String tableName) {
        return ss.get((tableName));
    }

    public void putTable(String tableName, TableMeta tableMeta) {
        ss.put(tableName, tableMeta);
    }

    public Collection<TableMeta> getAllTables() {
        return ss.values();
    }

    public static SchemaManager parseSchema(String data) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("schema is null");
        }

        InputStream sis = null;
        try {
            sis = new ByteArrayInputStream(data.getBytes());
            SchemaManager schemaManager = new LocalSchemaManager();
            List<TableMeta> schemaList = TableMetaParser.parseAll(sis);
            for (TableMeta t : schemaList) {
                schemaManager.putTable(t.getTableName(), t);
            }
            return schemaManager;
        } finally {
            IOUtils.closeQuietly(sis);
        }
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }
}
