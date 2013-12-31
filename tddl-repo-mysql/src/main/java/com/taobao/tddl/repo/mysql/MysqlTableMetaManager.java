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
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.executor.spi.IDataSourceGetter;
import com.taobao.tddl.optimizer.config.table.RepoSchemaManager;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.config.table.parse.TableMetaParser;
import com.taobao.tddl.repo.mysql.spi.DatasourceMySQLImplement;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author mengshi.sunmengshi 2013-12-5 下午6:18:14
 * @since 5.1.0
 */
@Activate(name = "MYSQL_JDBC", order = 2)
public class MysqlTableMetaManager extends RepoSchemaManager {

    private final static Logger logger   = LoggerFactory.getLogger(MysqlTableMetaManager.class);
    private IDataSourceGetter   dsGetter = new DatasourceMySQLImplement();

    public MysqlTableMetaManager(){
        this.setUseCache(false);
    }

    /**
     * 需要各Repo来实现
     * 
     * @param tableName
     */
    @Override
    protected TableMeta getTable0(String logicalTableName, String actualTableName) {

        TableMeta ts = fetchSchema(logicalTableName, actualTableName);

        return ts;
    }

    private TableMeta fetchSchema(String logicalTableName, String actualTableName) {

        DataSource ds = dsGetter.getDataSource(this.getGroup().getName());

        if (ds == null) {
            logger.error("schema of " + logicalTableName + " cannot be fetched, datasource is null, group name is "
                         + this.getGroup().getName());
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

            return this.resultSetMetaToSchema(rsmd, dbmd, logicalTableName, actualTableName);

        } catch (Exception e) {
            if (e instanceof SQLException) {
                if ("42000".equals(((SQLException) e).getSQLState())) {
                    try {
                        rs = stmt.executeQuery("select * from " + actualTableName + " where rownum<=2");
                        ResultSetMetaData rsmd = rs.getMetaData();
                        DatabaseMetaData dbmd = conn.getMetaData();

                        return this.resultSetMetaToSchema(rsmd, dbmd, logicalTableName, actualTableName);
                    } catch (SQLException e1) {
                        e1.printStackTrace();
                    }

                }
            }
            logger.error("schema of " + logicalTableName + " cannot be fetched", e);
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

    public static String xmlHead = "<tables xmlns=\"https://github.com/tddl/tddl/schema/table\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"https://github.com/tddl/tddl/schema/table https://raw.github.com/tddl/tddl/master/tddl-common/src/main/resources/META-INF/table.xsd\">";

    public static TableMeta resultSetMetaToSchema(ResultSetMetaData rsmd, DatabaseMetaData dbmd,
                                                  String logicalTableName, String actualTableName) {

        String xml = resultSetMetaToSchemaXml(rsmd, dbmd, logicalTableName, actualTableName);

        if (xml == null) return null;
        xml = xml.replaceFirst("<tables>", xmlHead);

        List<TableMeta> ts = null;

        ts = TableMetaParser.parse(xml);

        if (ts != null && !ts.isEmpty()) return ts.get(0);

        return null;
    }

    public static String resultSetMetaToSchemaXml(ResultSetMetaData rsmd, DatabaseMetaData dbmd,
                                                  String logicalTableName, String actualTableName) {

        try {

            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = null;
            // buil

            builder = dbf.newDocumentBuilder();

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
                logger.error("fetch schema error", e);
            }

            return null;
        } catch (Exception ex) {
            logger.error("fetch schema error", ex);
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
            xformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            xformer.transform(source, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
