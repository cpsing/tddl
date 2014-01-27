package com.taobao.tddl.optimizer.config.table;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.TddlToStringStyle;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.ConfigDataHandlerCity;
import com.taobao.tddl.optimizer.config.table.parse.TableMetaParser;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 有schema文件的schemamanager实现
 * 
 * @since 5.0.0
 */
public class StaticSchemaManager extends AbstractLifecycle implements SchemaManager {

    private final static Logger       logger          = LoggerFactory.getLogger(StaticSchemaManager.class);
    public final static MessageFormat schemaNullError = new MessageFormat("get schema null, appName is:{0}, unitName is:{1}, filePath is: {2}, dataId is: {3}");
    public final static MessageFormat SCHEMA_DATA_ID  = new MessageFormat("com.taobao.and_orV0.{0}_SCHEMA_DATAID");

    private ConfigDataHandler         schemaCdh;
    private String                    schemaFilePath  = null;
    private String                    appName         = null;
    private String                    unitName        = null;

    public StaticSchemaManager(String schemaFilePath, String appName, String unitName){
        super();
        this.schemaFilePath = schemaFilePath;
        this.appName = appName;
        this.unitName = unitName;
    }

    public StaticSchemaManager(){
    }

    protected Map<String, TableMeta> ss;

    protected void doInit() throws TddlException {
        super.doInit();
        ss = new ConcurrentHashMap<String, TableMeta>();

        if (this.appName == null) {
            logger.warn("schema appname is not assigned");
        }

        if (this.schemaFilePath == null) {
            logger.warn("schema file is not assigned");
        }

        if (appName == null && schemaFilePath == null) {
            return;
        }

        ConfigDataHandlerFactory factory = null;
        String dataId = null;

        // 优先从文件获取
        if (schemaFilePath == null) {
            dataId = SCHEMA_DATA_ID.format(new Object[] { appName });
            factory = ConfigDataHandlerCity.getFactory(appName, unitName);
        } else {
            factory = ConfigDataHandlerCity.getFileFactory(appName);
            dataId = schemaFilePath;
        }

        schemaCdh = factory.getConfigDataHandler(dataId, new SchemaConfigDataListener(this));

        String data = schemaCdh.getData(ConfigDataHandler.GET_DATA_TIMEOUT,
            ConfigDataHandler.FIRST_CACHE_THEN_SERVER_STRATEGY);

        if (data == null) {
            logger.warn(schemaNullError.format(new Object[] { appName, unitName, schemaFilePath, dataId }));
            return;
        }

        InputStream sis = null;
        try {
            sis = new ByteArrayInputStream(data.getBytes());
            List<TableMeta> schemaList = TableMetaParser.parse(sis);

            this.ss.clear();

            for (TableMeta table : schemaList) {
                this.putTable(table.getTableName(), table);
            }

            logger.warn("table fetched:");
            logger.warn(this.ss.keySet().toString());

        } catch (Exception e) {
            logger.error("table parser error, schema file is:\n" + data, e);
            throw new TddlRuntimeException(e);
        } finally {
            IOUtils.closeQuietly(sis);
        }

    }

    public static class SchemaConfigDataListener implements ConfigDataListener {

        private StaticSchemaManager schemaManager;

        public SchemaConfigDataListener(StaticSchemaManager schemaManager){
            this.schemaManager = schemaManager;
        }

        @Override
        public void onDataRecieved(String dataId, String data) {
            if (data == null || data.isEmpty()) {
                logger.warn("schema is null, dataId is " + dataId);
                return;
            }

            InputStream sis = null;
            try {
                sis = new ByteArrayInputStream(data.getBytes());
                List<TableMeta> schemaList = TableMetaParser.parse(sis);

                schemaManager.ss.clear();

                for (TableMeta table : schemaList) {
                    schemaManager.putTable(table.getTableName(), table);
                }

                logger.warn("table fetched:");
                logger.warn(schemaManager.ss.keySet().toString());
            } catch (Exception e) {
                logger.error("table parser error, schema file is:" + data, e);
                throw new TddlRuntimeException(e);
            } finally {
                IOUtils.closeQuietly(sis);
            }

        }

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

    public static StaticSchemaManager parseSchema(String data) throws TddlException {
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

    public static StaticSchemaManager parseSchema(InputStream in) throws TddlException {
        if (in == null) {
            throw new IllegalArgumentException("in is null");
        }

        try {
            StaticSchemaManager schemaManager = new StaticSchemaManager();
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
