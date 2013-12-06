package com.taobao.tddl.repo.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.repo.RepositoryHolder;
import com.taobao.tddl.executor.spi.DataSourceGetter;
import com.taobao.tddl.executor.spi.Repository;
import com.taobao.tddl.optimizer.config.Group;
import com.taobao.tddl.optimizer.config.table.TableMeta;

/**
 * @author mengshi.sunmengshi 2013-12-5 下午6:18:14
 * @since 5.1.0
 */
public class SchemaManagerDynamicImpl extends SchemaManager {

    private final static Log logger                      = LogFactory.getLog(SchemaManagerDynamicImpl.class);
    private final long       DEFAULT_SCHEMA_CLEAN_MINUTE = 10;
    private long             schemaCleanMinute           = DEFAULT_SCHEMA_CLEAN_MINUTE;
    Map<String, TableMeta>   schemas                     = new ConcurrentHashMap<String, TableMeta>();
    private Timer            timer;
    private DataSourceGetter dsGetter                    = null;

    static class SchemasCleaners extends java.util.TimerTask {

        private SchemaManagerDynamicImpl manager;

        public SchemasCleaners(SchemaManagerDynamicImpl manager){
            this.manager = manager;
        }

        @Override
        public void run() {
            manager.clearAllSchemas();
            logger.warn("TableSchema map has been cleaned");
        }
    }

    public SchemaManagerDynamicImpl(AndorContext oc){
        this.oc = oc;

        String dsGetterClass = StaticConfigConst.mysqlDataSourceGetter;

        if ("True".equalsIgnoreCase(GeneralUtil.getExtraCmd(oc.getConnectionProperties(),
            ExtraCmd.ConnectionExtraCmd.USE_TDHS_FOR_DEFAULT))) {
            dsGetterClass = StaticConfigConst.tdhsDataSourceGetter;
        }
        try {
            Class clz = Class.forName(dsGetterClass);
            // 如果schema data 为空，那么假定一定是个mysql。非mysql暂时还不支持动态schema .
            dsGetter = (DataSourceGetter) clz.newInstance();
        } catch (Exception ex) {
            logger.error("", ex);
            throw new IllegalArgumentException("can't find class " + dsGetterClass + " . "
                                               + "Do you import the ustore mysql/tdhs plug in your pom?", ex);
        }
        this.timer = new Timer();
        SchemasCleaners cleanTask = new SchemasCleaners(this);
        String cleanMinute = GeneralUtil.getExtraCmd(oc.getConnectionProperties(),
            ExtraCmd.ConnectionExtraCmd.CLEAN_SCHEMA_MINUTE);

        if (cleanMinute != null && !cleanMinute.isEmpty()) {
            try {
                schemaCleanMinute = Long.valueOf(cleanMinute);
            } catch (Exception e) {
                throw new RuntimeException("CLEAN_SCHEMA_MINUTE 格式不正确", e);
            }
        } else {
            schemaCleanMinute = DEFAULT_SCHEMA_CLEAN_MINUTE;
        }
        timer.schedule(cleanTask, 1000 * 60 * schemaCleanMinute, 1000 * 60 * schemaCleanMinute);
    }

    public AndorContext getOptimizerContext() {
        return oc;
    }

    @Override
    public TableSchema getSchema(String tablename) {
        TableSchema ts = schemas.get(tablename);
        if (ts != null) return ts;

        synchronized (this) {

            if (schemas.containsKey(tablename)) {
                return schemas.get(tablename);
            }

            ts = fetchSchema(tablename);

            if (ts == null) return null;
            this.schemas.put(tablename, ts);

            return ts;
        }
    }

    private TableSchema fetchSchema(String tablename) {
        RepositoryHolder holder = this.oc.getRepositoryHolder();

        Repository repo = null;

        if ("True".equalsIgnoreCase(GeneralUtil.getExtraCmd(oc.getConnectionProperties(),
            ExtraCmd.ConnectionExtraCmd.USE_TDHS_FOR_DEFAULT))) {
            repo = holder.get(Group.TDHS_CLIENT);

            if (repo == null) {
                throw new RuntimeException("tdhs repo未初始化，无法获取meta信息");
            }
        } else {
            repo = holder.get(Group.MY_JDBC);
            if (repo == null) {
                throw new RuntimeException("mysql repo未初始化，无法获取meta信息");
            }
        }

        String groupNode = null;

        if (!(oc.getRule() instanceof TddlRouteRule)) {
            throw new RuntimeException("使用tddl规则才可以自动构建meta");
        }
        Map<String, String> groupAndTableName = ((TddlRouteRule) oc.getRule()).getAGroupAndActualTableName(tablename);
        if (groupAndTableName == null) {
            throw new RuntimeException("table:" + tablename + " is not found in the rule\n" + "table:" + tablename
                                       + " 在规则中不存在，无法构建meta");
        }

        groupNode = groupAndTableName.keySet().iterator().next();

        if (groupNode == null) return null;

        String actualTableName = groupAndTableName.get(groupNode);
        DataSource ds = dsGetter.getDatasourceByGroupNode(repo.getCommonRuntimeConfigHolder(), groupNode);

        if (ds == null) {
            logger.error("schema of " + tablename + " cannot be fetched");
            return null;
        }

        Connection conn = null;
        Statement stmt = null;

        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select * from " + actualTableName + " limit 1");
            ResultSetMetaData rsmd = rs.getMetaData();
            DatabaseMetaData dbmd = conn.getMetaData();

            return TableSchemaParser.resultSetMetaToSchema(rsmd, dbmd, tablename, actualTableName);

        } catch (Exception e) {
            if (e instanceof SQLException) {
                if ("42000".equals(((SQLException) e).getSQLState())) {
                    try {
                        rs = stmt.executeQuery("select * from " + actualTableName + " where rownum<=2");
                        ResultSetMetaData rsmd = rs.getMetaData();
                        DatabaseMetaData dbmd = conn.getMetaData();

                        return TableSchemaParser.resultSetMetaToSchema(rsmd, dbmd, tablename, actualTableName);
                    } catch (SQLException e1) {
                        e1.printStackTrace();
                    }

                }
            }
            logger.error("schema of " + tablename + " cannot be fetched", e);
            return null;
        } finally {

            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                logger.warn(e);
                e.printStackTrace();
            }
        }

    }

    @Override
    public Collection<TableSchema> getAllSchemas() {
        return new ArrayList(this.schemas.values());
    }

    @Override
    public void putSchema(String tableName, TableSchema table) {
        this.schemas.put(table.getTableName(), table);
    }

    public void clearAllSchemas() {
        this.schemas.clear();
    }

    public static void main(String[] args) {
        ConcurrentHashMap<String, String> sa = new ConcurrentHashMap<String, String>();
        sa.put("Key", null);
    }
}
