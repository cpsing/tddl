package com.taobao.tddl.repo.mysql;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.taobao.tddl.executor.ExecutorContext;
import com.taobao.tddl.executor.repo.RepositoryHolder;
import com.taobao.tddl.executor.spi.IDataSourceGetter;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.optimizer.config.table.RepoSchemaManager;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.config.table.parse.TableMetaParser;
import com.taobao.tddl.repo.mysql.spi.DatasourceMySQLImplement;

/**
 * @author mengshi.sunmengshi 2013-12-5 下午6:18:14
 * @since 5.1.0
 */
public class MysqlTableMetaManager extends RepoSchemaManager {

    private final static Log  logger                      = LogFactory.getLog(MysqlTableMetaManager.class);
    private final long        DEFAULT_SCHEMA_CLEAN_MINUTE = 10;
    private IDataSourceGetter dsGetter                    = new DatasourceMySQLImplement();

    /**
     * 需要各Repo来实现
     * 
     * @param tableName
     */
    protected TableMeta getTable0(String tableName) {

        TableMeta ts = fetchSchema(tableName);

        return ts;
    }

    private TableMeta fetchSchema(String tablename) {
        RepositoryHolder holder = ExecutorContext.getContext().getRepositoryHolder();

        IRepository repo = null;

        DataSource ds = dsGetter.getDataSource(this.getGroup().getName());

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
            rs = stmt.executeQuery("select * from " + tablename + " limit 1");
            ResultSetMetaData rsmd = rs.getMetaData();
            DatabaseMetaData dbmd = conn.getMetaData();

            return this.resultSetMetaToSchema(rsmd, dbmd, tablename, tablename);

        } catch (Exception e) {
            if (e instanceof SQLException) {
                if ("42000".equals(((SQLException) e).getSQLState())) {
                    try {
                        rs = stmt.executeQuery("select * from " + actualTableName + " where rownum<=2");
                        ResultSetMetaData rsmd = rs.getMetaData();
                        DatabaseMetaData dbmd = conn.getMetaData();

                        return this.resultSetMetaToSchema(rsmd, dbmd, tablename, actualTableName);
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

    public static TableMeta resultSetMetaToSchema(ResultSetMetaData rsmd, DatabaseMetaData dbmd,
                                                  String logicalTableName, String actualTableName) {

        String xml = resultSetMetaToSchemaXml(rsmd, dbmd, logicalTableName, actualTableName);

        if (xml == null) return null;
        List<TableMeta> ts = null;
        try {
            ts = TableMetaParser.parse(xml);
        } catch (Exception e) {
            return null;
        }
        if (ts != null && !ts.isEmpty()) return ts.get(0);

        return null;
    }

    public static String resultSetMetaToSchemaXml(ResultSetMetaData rsmd, DatabaseMetaData dbmd,
                                                  String logicalTableName, String actualTableName) {

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = null;
            try {
                builder = dbf.newDocumentBuilder();
            } catch (Exception e) {
            }
            Document doc = builder.newDocument();
            Element tables = doc.createElement("tables");
            doc.appendChild(tables); // 将根元素添加到文档上
            Element table = doc.createElement("table");
            table.setAttribute("name", logicalTableName);
            tables.appendChild(table);
            Element columns = doc.createElement("columns");
            table.appendChild(columns);

            for (int i = 1; i <= rsmd.getColumnCount(); i++) {

                Element column = doc.createElement("column");
                column.setAttribute("name", rsmd.getColumnName(i));
                columns.appendChild(column);

                String type = TableMetaParser.jdbcTypeToAndorTypeString(rsmd.getColumnType(i));

                if (type == null) throw new IllegalArgumentException("列：" + rsmd.getColumnName(i) + " 类型"
                                                                     + rsmd.getColumnType(i) + "无法识别,联系沈询");

                column.setAttribute("type", type);
            }

            ResultSet pkrs = dbmd.getPrimaryKeys(null, null, actualTableName);

            if (pkrs.next()) {
                Element primaryKey = doc.createElement("primaryKey");
                primaryKey.appendChild(doc.createTextNode(pkrs.getString("COLUMN_NAME")));
                table.appendChild(primaryKey);
            } else {
                Element primaryKey = doc.createElement("primaryKey");
                primaryKey.appendChild(doc.createTextNode(rsmd.getColumnName(1)));
                table.appendChild(primaryKey);
            }

            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                OutputStreamWriter outwriter = new OutputStreamWriter(baos);

                callWriteXmlFile(doc, outwriter, "utf-8");
                outwriter.close();
                String content = baos.toString();
                return content;
            } catch (Exception e) {
                logger.error("", e);
                e.printStackTrace();
            }

            return null;
        } catch (Exception ex) {
            logger.error("", ex);
            return null;
        }

    }

    public static void callWriteXmlFile(Document doc, Writer w, String encoding) {
        try {
            Source source = new DOMSource(doc);
            Result result = new StreamResult(w);
            Transformer xformer = TransformerFactory.newInstance().newTransformer();
            xformer.setOutputProperty(OutputKeys.INDENT, "yes");
            xformer.setOutputProperty(OutputKeys.ENCODING, encoding);
            xformer.transform(source, result);
        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        }
    }
}
