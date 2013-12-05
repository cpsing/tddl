package com.taobao.tddl.repo.mysql.spi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map.Entry;

import javax.sql.DataSource;

import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.spi.Transaction;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.mysql.JDBC_Exception;

public class My_Table implements Table {

    protected DataSource ds;
    protected TableMeta  schema;
    protected String     groupNodeName;

    // 在插入或者更新的时候判断是否存在，如果存在则用insert否则update
    CloneableRecord      tmpKey   = null;
    // 判断是否已经做过了查询
    boolean              isSelect = false;

    public My_Table(DataSource ds, TableMeta schema, String groupNodeName){
        this.ds = ds;
        this.schema = schema;
        this.groupNodeName = groupNodeName;
    }

    public boolean isSelect() {
        return isSelect;
    }

    public void setSelect(boolean isSelect) {
        this.isSelect = isSelect;
    }

    @Override
    public TableMeta getSchema() {
        return schema;
    }

    public DataSource getDs() {
        return ds;
    }

    @Override
    public ISchematicCursor getCursor(Transaction txn, IndexMeta indexName, String isolation, IQuery executor)
                                                                                                              throws FetchException,
                                                                                                              SQLException {
        return null;
        /*
         * CursorMyUtils.getJdbcHandler(dsGetter, context, executor,
         * executionContext) My_JdbcHandler jdbcHandler = null; if (txn != null)
         * { if (!(txn instanceof My_Transaction)) { try { jdbcHandler =
         * ((My_Transaction) txn).getNewJdbcHandler(groupNodeName, ds, false); }
         * catch (SQLException e) { throw new FetchException(e); } } } else {
         * jdbcHandler = new My_JdbcHandler(); jdbcHandler.setDs(ds); }
         * ICursorMeta meta = GeneralUtil.convertToICursorMeta(indexName);
         * My_Cursor my_cursor = new My_Cursor(jdbcHandler, meta, groupNodeName,
         * executor, executor.isStreaming()); return new
         * SchematicMyCursor(my_cursor, meta,
         * CursorMyUtils.buildOrderBy(executor, indexName));
         */}

    @Override
    public void put(Transaction txn, CloneableRecord key, CloneableRecord value, IndexMeta indexMeta, String dbName)
                                                                                                                    throws Exception {
        StringBuilder putSb = null;

        putSb = new StringBuilder("insert into ");
        putSb.append(schema.getTableName()).append(" (");
        StringBuilder tmpSb = new StringBuilder(" values (");
        for (Entry<String, Object> en : value.getMap().entrySet()) {
            if (en.getValue() != null) {
                putSb.append(en.getKey()).append(",");
                tmpSb.append("'").append(en.getValue()).append("',");
            }
        }

        for (Entry<String, Object> en1 : key.getMap().entrySet()) {
            if (en1.getValue() != null) {
                putSb.append(en1.getKey()).append(",");
                tmpSb.append("'").append(en1.getValue()).append("',");
            }
        }

        putSb.delete(putSb.length() - 1, putSb.length());
        tmpSb.delete(tmpSb.length() - 1, tmpSb.length());

        putSb.append(") ").append(tmpSb).append(")");

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            ps = con.prepareStatement(putSb.toString());
            int res = ps.executeUpdate();
            if (res <= 0) {
                throw new JDBC_Exception("执行失败" + key.getColumnList());
            }
        } catch (SQLException e) {
            My_Log.getLog().error("error: 底层数据库异常", e);
            throw new JDBC_Exception("error: 底层数据库异常", e);
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                My_Log.getLog().warn("数据库关闭异常", e);
            }
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void delete(Transaction txn, CloneableRecord key, IndexMeta indexMeta, String dbName) throws Exception {
        StringBuilder delSb = new StringBuilder("delete from ");
        delSb.append(schema.getTableName()).append(" where ");
        if (key != null) {
            String and = "and";
            for (Entry<String, Object> en : key.getMap().entrySet()) {
                delSb.append(en.getKey()).append("='").append(en.getValue()).append("' ").append(and);
            }

            delSb.delete(delSb.length() - and.length(), delSb.length());
        } else {
            My_Log.getLog().warn("注意！删除了该表所有信息：" + schema.getTableName());
        }

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            ps = con.prepareStatement(delSb.toString());
            int res = ps.executeUpdate();
            if (res < 0) {
                throw new JDBC_Exception("执行失败:" + key.getColumnList() + "\n\r " + ps.getWarnings());
            }
        } catch (SQLException e) {
            My_Log.getLog().error("error: 底层数据库异常", e);
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                My_Log.getLog().warn("数据库关闭异常", e);
            }
        }
    }

    @Override
    public CloneableRecord get(Transaction txn, CloneableRecord key, IndexMeta indexMeta, String dbName) {
        throw new RuntimeException("暂时抛异常");
    }

    public long count() {
        return 0;
    }

    public void sync() {

    }

    public String getGroupNodeName() {
        return groupNodeName;
    }

    public void setGroupNodeName(String groupNodeName) {
        this.groupNodeName = groupNodeName;
    }

    @Override
    public ISchematicCursor getCursor(Transaction txn, IndexMeta indexMeta, String isolation, String indexMetaName)
                                                                                                                   throws FetchException {
        throw new UnsupportedOperationException();
    }

}
