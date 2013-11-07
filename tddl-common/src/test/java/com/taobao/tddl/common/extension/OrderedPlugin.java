package com.taobao.tddl.common.extension;

import com.taobao.tddl.common.utils.extension.Activate;

@Activate(order = 1)
public class OrderedPlugin extends AbstractPluginService {

    public String echo(String str) {
        return "ordered : " + str;
    }
}
