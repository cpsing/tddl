package com.taobao.tddl.atom;

import com.taobao.tddl.common.utils.TStringUtil;

/**
 * @author JIECHEN
 */
public enum AtomDbStatusEnum {

    R_STATUS(TAtomConstants.DB_STATUS_R), W_STATUS(TAtomConstants.DB_STATUS_W), RW_STATUS(TAtomConstants.DB_STATUS_RW),
    NA_STATUS(TAtomConstants.DB_STATUS_NA);

    private String status;

    AtomDbStatusEnum(String status){
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public static AtomDbStatusEnum getAtomDbStatusEnumByType(String type) {
        AtomDbStatusEnum statusEnum = null;
        if (TStringUtil.isNotBlank(type)) {
            String typeStr = type.toUpperCase().trim();
            if (typeStr.length() > 1) {
                if (AtomDbStatusEnum.NA_STATUS.getStatus().equals(typeStr)) {
                    statusEnum = AtomDbStatusEnum.NA_STATUS;
                } else if (!TStringUtil.contains(typeStr, AtomDbStatusEnum.NA_STATUS.getStatus())
                           && TStringUtil.contains(typeStr, AtomDbStatusEnum.R_STATUS.getStatus())
                           && TStringUtil.contains(typeStr, AtomDbStatusEnum.W_STATUS.getStatus())) {
                    statusEnum = AtomDbStatusEnum.RW_STATUS;
                }
            } else {
                if (AtomDbStatusEnum.R_STATUS.getStatus().equals(typeStr)) {
                    statusEnum = AtomDbStatusEnum.R_STATUS;
                } else if (AtomDbStatusEnum.W_STATUS.getStatus().equals(typeStr)) {
                    statusEnum = AtomDbStatusEnum.W_STATUS;
                }
            }
        }
        return statusEnum;
    }

    public boolean isNaStatus() {
        return this == AtomDbStatusEnum.NA_STATUS;
    }

    public boolean isRstatus() {
        return this == AtomDbStatusEnum.R_STATUS || this == AtomDbStatusEnum.RW_STATUS;
    }

    public boolean isWstatus() {
        return this == AtomDbStatusEnum.W_STATUS || this == AtomDbStatusEnum.RW_STATUS;
    }

    public boolean isRWstatus() {
        return this == AtomDbStatusEnum.RW_STATUS;
    }
}
