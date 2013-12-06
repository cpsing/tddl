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
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.utils.XmlHelper;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.IndexType;
import com.taobao.tddl.optimizer.config.table.Relationship;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * 基于xml定义{@linkplain TableMeta}，对应的解析器
 * 
 * @author jianghang 2013-11-12 下午6:11:17
 * @since 5.1.0
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
            toColumnMeta(primaryKeys, columnMetas),
            toColumnMeta(primaryValues, columnMetas),
            IndexType.BTREE,
            Relationship.NONE,
            strongConsistent,
            true,
            toColumnMeta(partitionColumns, columnMetas));

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
            toColumnMeta(keys, columnMetas),
            toColumnMeta(values, columnMetas),
            getIndexType(type),
            getRelationship(rel),
            strongConsistent,
            false,
            toColumnMeta(partitionColumns, columnMetas));
    }

    private static List<ColumnMeta> toColumnMeta(String[] columns, Map<String, ColumnMeta> columnMetas) {
        List<ColumnMeta> metas = Lists.newArrayList();
        for (int i = 0; i < columns.length; i++) {
            metas.add(columnMetas.get(columns[i].toUpperCase()));
        }

        return metas;
    }

    private static DATA_TYPE getDataType(String type) {
        if ("INT".equalsIgnoreCase(type)) {
            return DATA_TYPE.INT_VAL;
        } else if ("LONG".equalsIgnoreCase(type)) {
            return DATA_TYPE.LONG_VAL;
        } else if ("FLOAT".equalsIgnoreCase(type)) {
            return DATA_TYPE.FLOAT_VAL;
        } else if ("DOUBLE".equalsIgnoreCase(type)) {
            return DATA_TYPE.DOUBLE_VAL;
        } else if ("STRING".equalsIgnoreCase(type)) {
            return DATA_TYPE.STRING_VAL;
        } else if ("BYTES".equalsIgnoreCase(type)) {
            return DATA_TYPE.BYTES_VAL;
        } else if ("BOOLEAN".equalsIgnoreCase(type)) {
            return DATA_TYPE.BOOLEAN_VAL;
        } else if ("DATE".equalsIgnoreCase(type)) {
            return DATA_TYPE.DATE_VAL;
        } else if ("TIMESTAMP".equalsIgnoreCase(type)) {
            return DATA_TYPE.TIMESTAMP;
        } else if ("DATETIME".equalsIgnoreCase(type)) {
            return DATA_TYPE.TIMESTAMP;
        } else if ("BLOB".equalsIgnoreCase(type)) {
            return DATA_TYPE.BLOB;
        }

        throw new NotSupportException();
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

    public static DATA_TYPE jdbcTypeToAndorType(int jdbcType) {
        return getDataType(jdbcTypeToAndorTypeString(jdbcType));
    }

    public static String jdbcTypeToAndorTypeString(int jdbcType) {
        String type = null;
        switch (jdbcType) {
            case Types.BIGINT:
                type = "LONG";
                break;
            case Types.INTEGER:
            case Types.DECIMAL:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.NUMERIC:
                type = "INT";
                break;
            case Types.DATE:
                type = "DATE";
                break;
            case Types.TIMESTAMP:
                type = "TIMESTAMP";
                break;
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                type = "DOUBLE";
                break;
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
                type = "STRING";
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                type = "BYTES";
                break;
            case Types.CHAR:
                type = "STRING";
                break;
            case Types.BLOB:
                type = "BLOB";
                break;
            default:
                type = null;
        }

        return type;
    }

}
