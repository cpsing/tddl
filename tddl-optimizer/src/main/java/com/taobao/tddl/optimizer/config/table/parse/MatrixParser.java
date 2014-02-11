package com.taobao.tddl.optimizer.config.table.parse;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.collect.Lists;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.model.Atom;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.Group.GroupType;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.utils.XmlHelper;

/**
 * 解析matrix配置
 * 
 * @author jianghang 2013-11-28 下午6:08:57
 * @since 5.0.0
 */
public class MatrixParser {

    private static final String XSD_SCHEMA = "META-INF/matrix.xsd";

    public static Matrix parse(String data) {
        InputStream is = new ByteArrayInputStream(data.getBytes());
        return parse(is);
    }

    public static Matrix parse(InputStream in) {
        Document doc = XmlHelper.createDocument(in,
            Thread.currentThread().getContextClassLoader().getResourceAsStream(XSD_SCHEMA));
        Element root = doc.getDocumentElement();
        return parseMatrix(root);
    }

    private static Matrix parseMatrix(Element root) {
        Map<String, String> props = new HashMap<String, String>();
        NodeList childNodes = root.getChildNodes();
        Matrix matrix = new Matrix();

        for (int i = 0; i < childNodes.getLength(); i++) {
            Node item = childNodes.item(i);
            if ("properties".equals(item.getNodeName())) {
                props = parseProperties(item);
            } else if ("appName".equals(item.getNodeName())) {
                Node nameNode = item.getFirstChild();
                if (nameNode != null) {
                    matrix.setName(nameNode.getNodeValue());
                }
            }
        }

        NodeList groupList = root.getElementsByTagName("group");
        List<Group> groups = Lists.newArrayList();

        for (int i = 0; i < groupList.getLength(); i++) {
            groups.add(parseGroup(groupList.item(i), matrix.getName()));
        }

        matrix.setGroups(groups);
        matrix.setProperties(props);
        return matrix;
    }

    private static Group parseGroup(Node node, String appName) {
        Node nameNode = node.getAttributes().getNamedItem("name");
        Node typeNode = node.getAttributes().getNamedItem("type");
        Map<String, String> props = new HashMap<String, String>();
        List<Atom> atoms = new ArrayList<Atom>();

        NodeList childNodes = node.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node item = childNodes.item(i);
            if ("properties".equals(item.getNodeName())) {
                props = parseProperties(item);
            } else if ("atom".equals(item.getNodeName())) {
                atoms.add(parseAtom(item));
            }
        }
        Group group = new Group();
        if (nameNode != null) {
            group.setName(nameNode.getNodeValue());
        }

        if (typeNode != null) {
            group.setType(getGroupType(typeNode.getNodeValue()));
        }

        group.setAppName(appName);
        group.setProperties(props);
        group.setAtoms(atoms);
        return group;
    }

    private static Atom parseAtom(Node node) {
        Node nameNode = node.getAttributes().getNamedItem("name");
        Map<String, String> props = new HashMap<String, String>();
        NodeList childNodes = node.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node item = childNodes.item(i);
            if ("properties".equals(item.getNodeName())) {
                props = parseProperties(item);
            }

        }

        Atom atom = new Atom();
        if (nameNode != null) {
            atom.setName(nameNode.getNodeValue());
        }
        atom.setProperties(props);
        return atom;
    }

    private static Map<String, String> parseProperties(Node node) {
        Map<String, String> result = new HashMap<String, String>();
        NodeList childNodes = node.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node item = childNodes.item(i);
            if ("property".equals(item.getNodeName())) {
                Node nameNode = item.getAttributes().getNamedItem("name");
                Node valueNode = item.getAttributes().getNamedItem("value");
                if (nameNode == null || valueNode == null) {
                    NodeList itemChildNodes = item.getChildNodes();
                    for (int j = 0; j < itemChildNodes.getLength(); j++) {
                        Node itemChild = itemChildNodes.item(j);
                        if ("name".equals(itemChild.getNodeName())) {
                            nameNode = itemChild.getFirstChild();
                        } else if ("value".equals(itemChild.getNodeName())) {
                            valueNode = itemChild.getFirstChild();
                        }
                    }
                }

                result.put(nameNode.getNodeValue(), valueNode.getNodeValue());
            }
        }

        return result;
    }

    private static GroupType getGroupType(String type) {
        if ("MYSQL_JDBC".equalsIgnoreCase(type)) {
            return GroupType.MYSQL_JDBC;
        } else if ("MYSQL_ASYNC_JDBC".equalsIgnoreCase(type)) {
            return GroupType.MYSQL_ASYNC_JDBC;
        } else if ("JAVA_SKIPLIST".equalsIgnoreCase(type)) {
            return GroupType.JAVA_SKIPLIST;
        } else if ("HBASE_CLIENT".equalsIgnoreCase(type)) {
            return GroupType.HBASE_CLIENT;
        } else if ("TDHS_CLIENT".equalsIgnoreCase(type)) {
            return GroupType.TDHS_CLIENT;
        } else if ("ORACLE_JDBC".equalsIgnoreCase(type)) {
            return GroupType.ORACLE_JDBC;
        } else if ("BDB_JE".equalsIgnoreCase(type)) {
            return GroupType.BDB_JE;
        } else if ("OCEANBASE_JDBC".equalsIgnoreCase(type)) {
            return GroupType.OCEANBASE_JDBC;
        }

        throw new NotSupportException();
    }
}
