package com.taobao.tddl.optimizer.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 类似tddl三层结构的group概念，用于以后扩展第三方存储，目前扩展属性暂时使用properties代替
 * 
 * @author whisper
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 */
public class Group {

    private String name;

    public enum GroupType {
        BDB_JE, MYSQL_JDBC, MYSQL_ASYNC_JDBC, JAVA_SKIPLIST, HBASE_CLIENT, TDHS_CLIENT, ORACLE_JDBC;

        public boolean isMysql() {
            return this == MYSQL_JDBC;
        }
    }

    /**
     * 用于描述这组机器的类型
     */
    private GroupType           type       = GroupType.MYSQL_JDBC;

    private List<Atom>          atoms      = new ArrayList<Atom>();

    private Map<String, String> properties = new HashMap();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public GroupType getType() {
        return type;
    }

    public void setType(GroupType type) {
        this.type = type;
    }

    public List<Atom> getAtoms() {
        return atoms;
    }

    public Atom getAtom(String atomName) {
        for (Atom atom : atoms) {
            if (atom.getName().equals(atomName)) {
                return atom;
            }
        }

        throw new TddlRuntimeException("not found atomName : " + atomName);
    }

    public void setAtoms(List<Atom> atoms) {
        this.atoms = atoms;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }
}
