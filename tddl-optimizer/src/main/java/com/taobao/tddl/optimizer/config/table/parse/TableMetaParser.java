package com.taobao.tddl.optimizer.config.table.parse;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.collect.Lists;
import com.taobao.tddl.common.utils.XmlHelper;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.IndexType;
import com.taobao.tddl.optimizer.config.table.Relationship;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * 基于xml定义{@linkplain TableMeta}，对应的解析器
 * 
 * @author jianghang 2013-11-12 下午6:11:17
 * @since 5.0.0
 */
public class TableMetaParser {

    private static final String XSD_SCHEMA = "META-INF/table.xsd";

    /**
     * 基于数据流创建TableMeta对象
     * 
     * @param in
     * @return
     */
    public static List<TableMeta> parse(InputStream in) {
        Document doc = XmlHelper.createDocument(in,
            Thread.currentThread().getContextClassLoader().getResourceAsStream(XSD_SCHEMA));
        Element root = doc.getDocumentElement();
        NodeList tableNodeList = root.getElementsByTagName("table");
        List<TableMeta> tables = Lists.newArrayList();
        for (int i = 0; i < tableNodeList.getLength(); i++) {
            tables.add(parseTable(tableNodeList.item(i)));
        }

        return tables;
    }

    /**
     * 基于string的data文本创建TableMeta对象
     * 
     * @param data
     * @return
     */
    public static List<TableMeta> parse(String data) {
        InputStream is = new ByteArrayInputStream(data.getBytes());
        return parse(is);
    }

    // ================== helper method ================

    private static TableMeta parseTable(Node node) {
        Node nameNode = node.getAttributes().getNamedItem("name");
        String tableName = null;
        if (nameNode != null) {
            tableName = nameNode.getNodeValue().toUpperCase(); // 转化为大写
        }

        String[] primaryKeys = new String[0];
        String[] partitionColumns = new String[0];
        List<ColumnMeta> columns = Lists.newArrayList();
        List<IndexMeta> indexs = Lists.newArrayList();
        boolean strongConsistent = true;
        Map<String, ColumnMeta> columnMetas = new HashMap<String, ColumnMeta>();

        NodeList childs = node.getChildNodes();
        for (int i = 0; i < childs.getLength(); i++) {
            Node cnode = childs.item(i);
            if ("columns".equals(cnode.getNodeName())) {
                NodeList columnsChild = cnode.getChildNodes();
                for (int j = 0; j < columnsChild.getLength(); j++) {
                    Node ccnode = columnsChild.item(j);
                    if ("column".equals(ccnode.getNodeName())) {
                        columns.add(parseColumn(tableName, columnsChild.item(j)));
                    }
                }

                for (ColumnMeta column : columns) {
                    columnMetas.put(column.getName(), column);
                }
            } else if ("primaryKey".equals(cnode.getNodeName())) {
                primaryKeys = StringUtils.split(cnode.getFirstChild().getNodeValue(), ',');
            } else if ("partitionColumns".equals(cnode.getNodeName())) {
                partitionColumns = StringUtils.split(cnode.getFirstChild().getNodeValue(), ',');
            } else if ("strongConsistent".equals(cnode.getNodeName())) {
                strongConsistent = BooleanUtils.toBoolean(cnode.getFirstChild().getNodeValue());
            } else if ("secondaryIndexes".equals(cnode.getNodeName())) {
                NodeList indexsChild = cnode.getChildNodes();
                for (int j = 0; j < indexsChild.getLength(); j++) {
                    Node ccnode = indexsChild.item(j);
                    if ("indexMeta".equals(ccnode.getNodeName())) {
                        indexs.add(parseIndex(tableName, columnMetas, indexsChild.item(j)));
                    }
                }
            }
        }

        int i = 0;
        String[] primaryValues = new String[columns.size() - primaryKeys.length];
        for (ColumnMeta column : columns) {
            boolean c = false;
            for (String s : primaryKeys) {
                if (column.getName().equals(s.toUpperCase())) {
                    c = true;
                    break;
                }
            }
            if (!c) {
                primaryValues[i++] = column.getName();
            }
        }

        IndexMeta primaryIndex = new IndexMeta(tableName,
            toColumnMeta(primaryKeys, columnMetas, tableName),
            toColumnMeta(primaryValues, columnMetas, tableName),
            IndexType.BTREE,
            Relationship.NONE,
            strongConsistent,
            true,
            toColumnMeta(partitionColumns, columnMetas, tableName));

        return new TableMeta(tableName, columns, primaryIndex, indexs);
    }

    private static ColumnMeta parseColumn(String tableName, Node node) {
        Node nameNode = node.getAttributes().getNamedItem("name");
        Node typeNode = node.getAttributes().getNamedItem("type");
        Node aliasNode = node.getAttributes().getNamedItem("alias");
        Node nullableNode = node.getAttributes().getNamedItem("nullable");

        String name = null;
        String type = null;
        String alias = null;
        boolean nullable = true;
        if (nameNode != null) {
            name = nameNode.getNodeValue();
        }
        if (typeNode != null) {
            type = typeNode.getNodeValue();
        }
        if (aliasNode != null) {
            alias = aliasNode.getNodeValue();
        }
        if (nullableNode != null) {
            nullable = BooleanUtils.toBoolean(nullableNode.getNodeValue());
        }

        return new ColumnMeta(tableName, name, getDataType(type), alias, nullable);
    }

    private static IndexMeta parseIndex(String tableName, Map<String, ColumnMeta> columnMetas, Node node) {
        // Node nameNode = node.getAttributes().getNamedItem("name");
        Node typeNode = node.getAttributes().getNamedItem("type");
        Node relNode = node.getAttributes().getNamedItem("rel");
        Node strongConsistentNode = node.getAttributes().getNamedItem("strongConsistent");

        // String name = null;
        String type = null;
        String rel = null;
        boolean strongConsistent = true;
        // if (nameNode != null) {
        // name = nameNode.getNodeValue();
        // }
        if (typeNode != null) {
            type = typeNode.getNodeValue();
        }
        if (relNode != null) {
            rel = relNode.getNodeValue();
        }
        if (strongConsistentNode != null) {
            strongConsistent = BooleanUtils.toBoolean(strongConsistentNode.getNodeValue());
        }

        String[] keys = new String[0];
        String[] values = new String[0];
        String[] partitionColumns = new String[0];
        NodeList childs = node.getChildNodes();
        for (int i = 0; i < childs.getLength(); i++) {
            Node cnode = childs.item(i);
            if ("keys".equals(cnode.getNodeName())) {
                keys = StringUtils.split(cnode.getFirstChild().getNodeValue(), ',');
            } else if ("values".equals(cnode.getNodeName())) {
                values = StringUtils.split(cnode.getFirstChild().getNodeValue(), ',');
            } else if ("partitionColumns".equals(cnode.getNodeName())) {
                partitionColumns = StringUtils.split(cnode.getFirstChild().getNodeValue(), ',');
            }
        }

        return new IndexMeta(tableName,
            toColumnMeta(keys, columnMetas, tableName),
            toColumnMeta(values, columnMetas, tableName),
            getIndexType(type),
            getRelationship(rel),
            strongConsistent,
            false,
            toColumnMeta(partitionColumns, columnMetas, tableName));
    }

    private static List<ColumnMeta> toColumnMeta(String[] columns, Map<String, ColumnMeta> columnMetas, String tableName) {
        List<ColumnMeta> metas = Lists.newArrayList();
        for (int i = 0; i < columns.length; i++) {

            String cname = columns[i].toUpperCase();

            if (!columnMetas.containsKey(cname)) {
                throw new RuntimeException("column: " + cname + ", is not a column of table " + tableName);
            }

            metas.add(columnMetas.get(cname));
        }

        return metas;
    }

    private static IndexType getIndexType(String type) {
        if ("BTREE".equalsIgnoreCase(type)) {
            return IndexType.BTREE;
        } else if ("HASH".equalsIgnoreCase(type)) {
            return IndexType.HASH;
        } else if ("INVERSE".equalsIgnoreCase(type)) {
            return IndexType.INVERSE;
        }
        return IndexType.NONE;
    }

    private static Relationship getRelationship(String rel) {
        if ("MANY_TO_MANY".equalsIgnoreCase(rel)) {
            return Relationship.MANY_TO_MANY;
        } else if ("ONE_TO_ONE".equalsIgnoreCase(rel)) {
            return Relationship.ONE_TO_ONE;
        } else if ("MANY_TO_ONE".equalsIgnoreCase(rel)) {
            return Relationship.MANY_TO_ONE;
        } else if ("ONE_TO_MANY".equalsIgnoreCase(rel)) {
            return Relationship.ONE_TO_MANY;
        }

        return Relationship.NONE;
    }

    public static DataType jdbcTypeToDataType(int jdbcType) {
        return getDataType(jdbcTypeToDataTypeString(jdbcType));
    }

    private static DataType getDataType(String type) {
        if ("INT".equalsIgnoreCase(type) || "INTEGER".equalsIgnoreCase(type)) {
            return DataType.IntegerType;
        } else if ("LONG".equalsIgnoreCase(type)) {
            return DataType.LongType;
        } else if ("SHORT".equalsIgnoreCase(type)) {
            return DataType.ShortType;
        } else if ("BIGINTEGER".equalsIgnoreCase(type)) {
            return DataType.BigIntegerType;
        } else if ("BIGDECIMAL".equalsIgnoreCase(type)) {
            return DataType.BigDecimalType;
        } else if ("FLOAT".equalsIgnoreCase(type)) {
            return DataType.FloatType;
        } else if ("DOUBLE".equalsIgnoreCase(type)) {
            return DataType.DoubleType;
        } else if ("STRING".equalsIgnoreCase(type)) {
            return DataType.StringType;
        } else if ("BYTES".equalsIgnoreCase(type)) {
            return DataType.BytesType;
        } else if ("BOOLEAN".equalsIgnoreCase(type)) {
            return DataType.BooleanType;
        } else if ("DATE".equalsIgnoreCase(type)) {
            return DataType.DateType;
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return DataType.TimestampType;
        } else if ("DATETIME".equalsIgnoreCase(type)) {
            return DataType.DatetimeType;
        } else if ("TIME".equalsIgnoreCase(type)) {
            return DataType.TimeType;
        } else if ("BLOB".equalsIgnoreCase(type)) {
            return DataType.BlobType;
        } else if ("BIT".equalsIgnoreCase(type)) {
            return DataType.BitType;
        } else {
            throw new IllegalArgumentException("不支持的类型：" + type);
        }
    }

    public static String jdbcTypeToDataTypeString(int jdbcType) {
        String type = null;
        switch (jdbcType) {
            case Types.BIGINT:
                // 考虑unsigned
                type = "BIGINTEGER";
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                type = "BIGDECIMAL";
                break;
            case Types.INTEGER:
                // 考虑unsigned
                type = "LONG";
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
                // 考虑unsigned
                type = "INT";
                break;
            case Types.DATE:
                type = "DATE";
                break;
            case Types.TIMESTAMP:
                type = "TIMESTAMP";
                break;
            case Types.TIME:
                type = "TIME";
                break;
            case Types.FLOAT:
                type = "FLOAT";
                break;
            case Types.REAL:
            case Types.DOUBLE:
                type = "DOUBLE";
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                type = "STRING";
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                type = "BYTES";
                break;
            case Types.BLOB:
                type = "BLOB";
                break;
            case Types.BIT:
                type = "BIT";
                break;
            default:
                throw new IllegalArgumentException("不支持的类型：" + jdbcType);
        }

        return type;
    }

}
