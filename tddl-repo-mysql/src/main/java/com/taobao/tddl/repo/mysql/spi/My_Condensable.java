package com.taobao.tddl.repo.mysql.spi;

/**
 * 用于标记该Cursor是否可被收缩。
 * 
 * @author whisper
 */
public interface My_Condensable {

    /**
     * @return 如果可以在单机直接使用sql.返回对应groupname，如果不能直接在单机的，返回null
     */
    public String getGroupNodeName();

    // /**
    // * 目前andor内表名类似【 realTable】.【indexName】_【number_suffix】
    // *
    // * @return 如果可以在单机进行，那么返回真实表。如果不能,返回空
    // */
    // public boolean canCondense();
    // public OneQuery getSql();
}
