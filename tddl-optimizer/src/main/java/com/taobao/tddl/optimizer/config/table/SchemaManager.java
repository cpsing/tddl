package com.taobao.tddl.optimizer.config.table;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;


/**
 * 用来描述一个逻辑表由哪些key-val组成的 <br/>
 * 屏蔽掉不同的schema存储，存储可能会是本地,diamond或zk schema
 * 
 * @author jianxing <jianxing.qx@taobao.com>
 * @author whisper
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 */
public abstract class SchemaManager {

    public abstract TableMeta getTable(String tableName);

    public abstract void putTable(String tableName, TableMeta tableMeta);

    public abstract Collection<TableMeta> getAllTables();

    public static class LocalSchemaManager extends SchemaManager {

        public String toString() {
            return "LSchemaManager [ss=" + ss + "]";
        }

        Map<String, TableMeta> ss = new ConcurrentHashMap<String, TableMeta>();

        public TableMeta getTable(String tableName) {
            return ss.get((tableName));
        }

        public void putTable(String tableName, TableMeta tableMeta) {
            ss.put(tableName, tableMeta);
        }

        public Collection<TableMeta> getAllTables() {
            return ss.values();
        }
    }

    public static SchemaManager parseSchema(String data) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("schema is null");
        }

        InputStream sis = null;
        try {
            sis = new ByteArrayInputStream(data.getBytes());
            SchemaManager schemaManager = new SchemaManager.LocalSchemaManager();
            List<TableMeta> schemaList = TableMetaParser.parseAll(sis);
            for (TableMeta t : schemaList) {
                schemaManager.putTable(t.getTableName(), t);
            }
            return schemaManager;
        } finally {
            IOUtils.closeQuietly(sis);
        }
    }

}
