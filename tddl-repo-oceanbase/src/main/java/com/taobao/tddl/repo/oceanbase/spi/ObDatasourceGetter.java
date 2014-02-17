package com.taobao.tddl.repo.oceanbase.spi;

import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.repo.mysql.spi.DatasourceMySQLImplement;

public class ObDatasourceGetter extends DatasourceMySQLImplement {

    @Override
    protected boolean isNotValidateNode(Group.GroupType type) {
        return super.isNotValidateNode(type) && !Group.GroupType.OCEANBASE_JDBC.equals(type);
    }

}
