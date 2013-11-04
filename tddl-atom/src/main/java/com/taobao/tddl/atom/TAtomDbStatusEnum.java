package com.taobao.tddl.atom;

import com.taobao.tddl.common.utils.TStringUtil;

/**
 * @author JIECHEN
 */
public enum TAtomDbStatusEnum {

    R_STATUS(TAtomConstants.DB_STATUS_R), W_STATUS(TAtomConstants.DB_STATUS_W), RW_STATUS(TAtomConstants.DB_STATUS_RW),
    NA_STATUS(TAtomConstants.DB_STATUS_NA);

    private String status;

    TAtomDbStatusEnum(String status){
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public static TAtomDbStatusEnum getAtomDbStatusEnumByType(String type) {
        TAtomDbStatusEnum statusEnum = null;
        if (TStringUtil.isNotBlank(type)) {
            String typeStr = type.toUpperCase().trim();
            if (typeStr.length() > 1) {
                if (TAtomDbStatusEnum.NA_STATUS.getStatus().equals(typeStr)) {
                    statusEnum = TAtomDbStatusEnum.NA_STATUS;
                } else if (!TStringUtil.contains(typeStr, TAtomDbStatusEnum.NA_STATUS.getStatus())
                           && TStringUtil.contains(typeStr, TAtomDbStatusEnum.R_STATUS.getStatus())
                           && TStringUtil.contains(typeStr, TAtomDbStatusEnum.W_STATUS.getStatus())) {
                    statusEnum = TAtomDbStatusEnum.RW_STATUS;
                }
            } else {
                if (TAtomDbStatusEnum.R_STATUS.getStatus().equals(typeStr)) {
                    statusEnum = TAtomDbStatusEnum.R_STATUS;
                } else if (TAtomDbStatusEnum.W_STATUS.getStatus().equals(typeStr)) {
                    statusEnum = TAtomDbStatusEnum.W_STATUS;
                }
            }
        }
        return statusEnum;
    }

    public boolean isNaStatus() {
        return this == TAtomDbStatusEnum.NA_STATUS;
    }

    public boolean isRstatus() {
        return this == TAtomDbStatusEnum.R_STATUS || this == TAtomDbStatusEnum.RW_STATUS;
    }

    public boolean isWstatus() {
        return this == TAtomDbStatusEnum.W_STATUS || this == TAtomDbStatusEnum.RW_STATUS;
    }

    public boolean isRWstatus() {
        return this == TAtomDbStatusEnum.RW_STATUS;
    }
}
