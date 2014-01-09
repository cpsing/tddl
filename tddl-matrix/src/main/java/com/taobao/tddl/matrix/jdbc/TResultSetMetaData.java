package com.taobao.tddl.matrix.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * @author mengshi.sunmengshi 2013-12-3 下午6:25:57
 * @since 5.1.0
 */
public class TResultSetMetaData implements ResultSetMetaData {

    private List<ColumnMeta> columnMetas;

    public TResultSetMetaData(List<ColumnMeta> columns){
        this.columnMetas = columns;
    }

    public boolean columnIsExist(String column) {
        for (ColumnMeta metaItem : columnMetas) {
            if (column.equalsIgnoreCase(metaItem.getName()) || column.equalsIgnoreCase(metaItem.getAlias())) {
                return true;
            }
        }
        return false;
    }

    public DATA_TYPE getColumnDataType(String column) {
        for (ColumnMeta metaItem : columnMetas)
            if (column.equalsIgnoreCase(metaItem.getName()) || column.equalsIgnoreCase(metaItem.getAlias())) {
                return metaItem.getDataType();
            }
        return null;
    }

    public int getColumnCount() throws SQLException {
        return this.columnMetas.size();
    }

    // jdbc规范从1开始，database从0开始，所以减一
    public String getColumnName(int column) throws SQLException {
        column--;
        ColumnMeta columnObj = this.columnMetas.get(column);
        return columnObj.getName();
    }

    public String getColumnLabel(int column) throws SQLException {
        column--;
        return TStringUtil.isBlank(this.columnMetas.get(column).getAlias()) ? this.columnMetas.get(column).getName() : this.columnMetas.get(column)
            .getAlias();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.getClass().isAssignableFrom(iface);
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    public int isNullable(int column) throws SQLException {
        return 0;
    }

    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        return 20;
    }

    public String getSchemaName(int column) throws SQLException {
        return "Tddl";
    }

    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    public int getScale(int column) throws SQLException {
        return 0;
    }

    public String getTableName(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);
        return c.getTableName();
    }

    public String getCatalogName(int column) throws SQLException {
        return "Tddl";
    }

    public int getColumnType(int column) throws SQLException {
        return 0;
    }

    public String getColumnTypeName(int column) throws SQLException {
        column--;
        ColumnMeta c = this.columnMetas.get(column);
        return c.getDataType().toString();
    }

    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    public boolean isWritable(int column) throws SQLException {
        return true;
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    public String getColumnClassName(int column) throws SQLException {
        return null;
    }

    public List<ColumnMeta> getColumnMetas() {
        return columnMetas;
    }

    public void setColumnMetas(List<ColumnMeta> columnMetas) {
        this.columnMetas = columnMetas;
    }

}
