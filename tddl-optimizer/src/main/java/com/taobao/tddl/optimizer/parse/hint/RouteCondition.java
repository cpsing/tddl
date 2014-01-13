package com.taobao.tddl.optimizer.parse.hint;

import java.util.Map;

public interface RouteCondition {

    public String getVirtualTableName();

    public void setVirtualTableName(String virtualTableName);

    public Map<String, Object> getExtraCmds();
}
