package com.taobao.tddl.common.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 类似tddl三层结构的matrix概念，用于以后扩展第三方存储，目前扩展属性暂时使用properties代替
 * 
 * @author whisper
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 */
public class Matrix {

    private String              name;

    private List<Group>         groups     = new ArrayList<Group>();

    private Map<String, String> properties = new HashMap();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Group> getGroups() {
        return groups;
    }

    public void setGroups(List<Group> groups) {
        this.groups = groups;
    }

    public Group getGroup(String groupName) {
        for (Group group : groups) {
            if (group.getName().equals(groupName)) {
                return group;
            }
        }

        throw new TddlRuntimeException("not found groupName : " + groupName);
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
