package com.taobao.tddl.optimizer.config.table;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.TddlToStringStyle;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.optimizer.config.table.parse.TableMetaParser;

/**
 * 本地文件的schema manager实现
 * 
 * @since 5.1.0
 */
public class LocalSchemaManager extends AbstractLifecycle implements SchemaManager {

    private ConfigDataHandler        schemaCdh;
    private String                   schemaFilePath = null;
    private String                   appName        = null;

    protected Map<String, TableMeta> ss;

    protected void doInit() throws TddlException {
        super.doInit();
        ss = new ConcurrentHashMap<String, TableMeta>();
    }

    protected void doDestory() throws TddlException {
        super.doDestory();
        ss.clear();
    }

    public TableMeta getTable(String tableName) {
        return ss.get((tableName));
    }

    public void putTable(String tableName, TableMeta tableMeta) {
        ss.put(tableName.toUpperCase(), tableMeta);
    }

    public Collection<TableMeta> getAllTables() {
        return ss.values();
    }

    public static LocalSchemaManager parseSchema(String data) throws TddlException {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("schema is null");
        }

        InputStream sis = null;
        try {
            sis = new ByteArrayInputStream(data.getBytes());
            return parseSchema(sis);
        } finally {
            IOUtils.closeQuietly(sis);
        }
    }

    public static LocalSchemaManager parseSchema(InputStream in) throws TddlException {
        if (in == null) {
            throw new IllegalArgumentException("in is null");
        }

        try {
            LocalSchemaManager schemaManager = new LocalSchemaManager();
            schemaManager.init();
            List<TableMeta> schemaList = TableMetaParser.parse(in);
            for (TableMeta t : schemaList) {
                schemaManager.putTable(t.getTableName(), t);
            }
            return schemaManager;
        } finally {
            IOUtils.closeQuietly(in);
        }

    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }
}
