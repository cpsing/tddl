package com.taobao.tddl.executor.spi;

public class RemotingExecutor {

    /**
     * 对应的group名字
     */
    private String groupName;
    /**
     * 类型
     */
    private String type;
    /**
     * 可能是个datasource ，也可能是个rpc客户端。放在一起的原因是
     */
    private Object remotingExecutableObject;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Object getRemotingExecutableObject() {
        return remotingExecutableObject;
    }

    public void setRemotingExecutableObject(Object remotingExecutableObject) {
        this.remotingExecutableObject = remotingExecutableObject;
    }

    @Override
    public String toString() {
        return "RemotingExecutor [groupName=" + groupName + ", type=" + type + ", remotingExecutableObject="
               + remotingExecutableObject + "]";
    }

}
