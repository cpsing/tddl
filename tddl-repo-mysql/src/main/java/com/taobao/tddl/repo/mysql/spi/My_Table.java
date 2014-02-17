package com.taobao.tddl.repo.mysql.spi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.sql.DataSource;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.spi.IDataSourceGetter;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.mysql.cursor.SchematicMyCursor;
import com.taobao.tddl.repo.mysql.utils.MysqlRepoUtils;

public class My_Table implements ITable {

    private static final Logger log      = LoggerFactory.getLogger(My_Table.class);
    protected DataSource        ds;
    protected TableMeta         schema;
    protected String            groupNodeName;
    protected IDataSourceGetter dsGetter = new DatasourceMySQLImplement();
    // 在插入或者更新的时候判断是否存在，如果存在则用insert否则update
    protected CloneableRecord   tmpKey   = null;
    // 判断是否已经做过了查询
    protected boolean           isSelect = false;

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
    public ISchematicCursor getCursor(ExecutionContext executionContext, IndexMeta indexName, IQuery executor)
                                                                                                              throws TddlException {
        My_JdbcHandler jdbcHandler = MysqlRepoUtils.getJdbcHandler(this.dsGetter, executor, executionContext);
        ICursorMeta meta = ExecUtils.convertToICursorMeta(indexName);
        My_Cursor my_cursor = new My_Cursor(jdbcHandler, meta, executor, executor.isStreaming());
        return new SchematicMyCursor(my_cursor, meta, MysqlRepoUtils.buildOrderBy(executor, indexName));
    }

    @Override
    public void put(ExecutionContext executionContext, CloneableRecord key, CloneableRecord value, IndexMeta indexMeta,
                    String dbName) throws TddlException {
        StringBuilder putSb = null;

        putSb = new StringBuilder("replace into ");
        putSb.append(schema.getTableName()).append(" (");
        StringBuilder tmpSb = new StringBuilder(" values (");
        List values = new ArrayList();
        for (Entry<String, Object> en : value.getMap().entrySet()) {
            if (en.getValue() != null) {
                putSb.append(en.getKey()).append(",");

                values.add(en.getValue());

                tmpSb.append("").append("?").append(",");
            }
        }

        for (Entry<String, Object> en1 : key.getMap().entrySet()) {
            if (en1.getValue() != null) {
                putSb.append(en1.getKey()).append(",");

                values.add(en1.getValue());

                tmpSb.append("").append("?").append(",");
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

            int i = 1;
            for (Object v : values) {
                ps.setObject(i++, v);
            }
            int res = ps.executeUpdate();
            if (res <= 0) {
                throw new TddlException("执行失败" + key.getColumnList());
            }
        } catch (SQLException e) {
            log.error("error: 底层数据库异常", e);
            throw new TddlException("error: 底层数据库异常", e);
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                log.warn("数据库关闭异常", e);
            }

            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                log.warn("数据库关闭异常", e);
            }
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void delete(ExecutionContext executionContext, CloneableRecord key, IndexMeta indexMeta, String dbName)
                                                                                                                  throws TddlException {
        StringBuilder delSb = new StringBuilder("delete from ");
        delSb.append(schema.getTableName()).append(" where ");
        if (key != null) {
            String and = " and ";
            int size = key.getMap().size();
            int i = 0;
            for (Entry<String, Object> en : key.getMap().entrySet()) {
                delSb.append(en.getKey()).append("='").append(en.getValue()).append("'");
                if (++i < size) {
                    delSb.append(and);
                }
            }
        } else {
            log.warn("注意！删除了该表所有信息：" + schema.getTableName());
        }

        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ds.getConnection();
            ps = con.prepareStatement(delSb.toString());
            int res = ps.executeUpdate();
            if (res < 0) {
                throw new TddlException("执行失败:" + key.getColumnList() + "\n\r " + ps.getWarnings());
            }
        } catch (SQLException e) {
            log.error("error: 底层数据库异常", e);
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                log.warn("数据库关闭异常", e);
            }

            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                log.warn("数据库关闭异常", e);
            }
        }
    }

    @Override
    public CloneableRecord get(ExecutionContext executionContext, CloneableRecord key, IndexMeta indexMeta,
                               String dbName) {
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
    public ISchematicCursor getCursor(ExecutionContext executionContext, IndexMeta indexMeta, String indexMetaName)
                                                                                                                   throws TddlException {
        throw new UnsupportedOperationException();
    }

}
