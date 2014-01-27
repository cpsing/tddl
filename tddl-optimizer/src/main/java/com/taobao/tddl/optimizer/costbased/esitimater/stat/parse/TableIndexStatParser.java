package com.taobao.tddl.optimizer.costbased.esitimater.stat.parse;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.collect.Lists;
import com.taobao.tddl.common.utils.XmlHelper;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.KVIndexStat;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.TableIndexStat;

/**
 * 解析matrix配置
 * 
 * @author jianghang 2013-11-28 下午6:08:57
 * @since 5.0.0
 */
public class TableIndexStatParser {

    private static final String XSD_SCHEMA = "META-INF/stat.xsd";

    public static List<TableIndexStat> parse(String data) {
        InputStream is = new ByteArrayInputStream(data.getBytes());
        return parse(is);
    }

    public static List<TableIndexStat> parse(InputStream in) {
        Document doc = XmlHelper.createDocument(in,
            Thread.currentThread().getContextClassLoader().getResourceAsStream(XSD_SCHEMA));
        Element root = doc.getDocumentElement();
        List<TableIndexStat> stats = Lists.newArrayList();
        NodeList list = root.getElementsByTagName("tableIndexStat");
        for (int i = 0; i < list.getLength(); i++) {
            Node item = list.item(i);
            stats.add(parseTableIndexStat(item));
        }

        return stats;
    }

    private static TableIndexStat parseTableIndexStat(Node node) {
        NodeList list = node.getChildNodes();
        TableIndexStat stat = new TableIndexStat();
        for (int i = 0; i < list.getLength(); i++) {
            Node item = list.item(i);
            if ("tableName".equals(item.getNodeName())) {
                stat.setTableName(StringUtils.upperCase(item.getFirstChild().getNodeValue()));
            } else if ("indexStats".equals(item.getNodeName())) {
                NodeList indexChilds = item.getChildNodes();
                for (int j = 0; j < indexChilds.getLength(); j++) {
                    Node indexItem = indexChilds.item(j);
                    if ("indexStat".equals(indexItem.getNodeName())) {
                        stat.addKVIndexStat(parseKVIndexStat(indexItem));
                    }
                }
            }
        }

        return stat;
    }

    private static KVIndexStat parseKVIndexStat(Node node) {
        NodeList list = node.getChildNodes();
        KVIndexStat stat = new KVIndexStat();
        for (int i = 0; i < list.getLength(); i++) {
            Node item = list.item(i);
            if ("indexName".equals(item.getNodeName())) {
                stat.setIndexName(StringUtils.upperCase(item.getFirstChild().getNodeValue()));
            } else if ("indexType".equals(item.getNodeName())) {
                stat.setIndexType(Integer.valueOf(item.getFirstChild().getNodeValue()));
            } else if ("distinctKeys".equals(item.getNodeName())) {
                stat.setDistinctKeys(Integer.valueOf(item.getFirstChild().getNodeValue()));
            } else if ("numRows".equals(item.getNodeName())) {
                stat.setNumRows(Integer.valueOf(item.getFirstChild().getNodeValue()));
            } else if ("factor".equals(item.getNodeName())) {
                stat.setFactor(Double.valueOf(item.getFirstChild().getNodeValue()));
            }
        }

        return stat;
    }
}
