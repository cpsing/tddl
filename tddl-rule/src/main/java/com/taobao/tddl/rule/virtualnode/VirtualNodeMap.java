package com.taobao.tddl.rule.virtualnode;

import com.taobao.tddl.common.model.lifecycle.Lifecycle;

/**
 * 虚拟节点
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-8-8下午07:08:45
 */
public interface VirtualNodeMap extends Lifecycle {

    String tableSplitor = "_";

    public String getValue(String key);
}
