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
import com.taobao.tddl.optimizer.costbased.esitimater.stat.TableStat;

/**
 * 解析matrix配置
 * 
 * @author jianghang 2013-11-28 下午6:08:57
 * @since 5.0.0
 */
public class TableStatParser {

    private static final String XSD_SCHEMA = "META-INF/stat.xsd";

    public static List<TableStat> parse(String data) {
        InputStream is = new ByteArrayInputStream(data.getBytes());
        return parse(is);
    }

    public static List<TableStat> parse(InputStream in) {
        Document doc = XmlHelper.createDocument(in,
            Thread.currentThread().getContextClassLoader().getResourceAsStream(XSD_SCHEMA));
        Element root = doc.getDocumentElement();

        List<TableStat> stats = Lists.newArrayList();
        NodeList list = root.getElementsByTagName("tableStat");
        for (int i = 0; i < list.getLength(); i++) {
            Node item = list.item(i);
            stats.add(parseTableStat(item));
        }
        return stats;
    }

    private static TableStat parseTableStat(Node node) {
        NodeList list = node.getChildNodes();
        TableStat stat = new TableStat();
        for (int i = 0; i < list.getLength(); i++) {
            Node item = list.item(i);
            if ("tableName".equals(item.getNodeName())) {
                stat.setTableName(StringUtils.upperCase(item.getFirstChild().getNodeValue()));
            } else if ("tableRows".equals(item.getNodeName())) {
                stat.setTableRows(Long.valueOf(item.getFirstChild().getNodeValue()));
            }

        }

        return stat;
    }
}
