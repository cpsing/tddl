package com.taobao.tddl.group.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

/**
 * @author linxuan
 */
public class TGroupDatabaseMetaData implements DatabaseMetaData {

    protected TGroupConnection tGroupConnection = null;
    private TGroupDataSource   tGroupDataSource;

    public TGroupDatabaseMetaData(TGroupConnection tGroupConnection, TGroupDataSource tGroupDataSource){
        super();
        this.tGroupConnection = tGroupConnection;
        this.tGroupDataSource = tGroupDataSource;
    }

    public DatabaseMetaData getDatabaseMetaData() throws SQLException {
        Connection conn = null;
        DatabaseMetaData dbMa;
        try {
            // conn = tGroupConnection.createNewConnection(wBaseDsWrapper,
            // false);
            conn = tGroupDataSource.getDBSelector(false).select().getConnection();
            dbMa = conn.getMetaData();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return dbMa;

    }

    public boolean allProceduresAreCallable() throws SQLException {
        return getDatabaseMetaData().allProceduresAreCallable();
    }

    public boolean allTablesAreSelectable() throws SQLException {
        return getDatabaseMetaData().allTablesAreSelectable();
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return getDatabaseMetaData().autoCommitFailureClosesAllResultSets();
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return getDatabaseMetaData().dataDefinitionCausesTransactionCommit();
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return getDatabaseMetaData().dataDefinitionIgnoredInTransactions();
    }

    public boolean deletesAreDetected(int type) throws SQLException {
        return getDatabaseMetaData().deletesAreDetected(type);
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return getDatabaseMetaData().doesMaxRowSizeIncludeBlobs();
    }

    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return getDatabaseMetaData().getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
                                                                                                                   throws SQLException {
        return getDatabaseMetaData().getBestRowIdentifier(catalog, schema, table, scope, nullable);
    }

    public String getCatalogSeparator() throws SQLException {
        return getDatabaseMetaData().getCatalogSeparator();
    }

    public String getCatalogTerm() throws SQLException {
        return getDatabaseMetaData().getCatalogTerm();
    }

    public ResultSet getCatalogs() throws SQLException {
        return getDatabaseMetaData().getCatalogs();
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        return getDatabaseMetaData().getClientInfoProperties();
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
                                                                                                               throws SQLException {
        return getDatabaseMetaData().getColumnPrivileges(catalog, schema, table, columnNamePattern);
    }

    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
                                                                                                                        throws SQLException {
        return getDatabaseMetaData().getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    public Connection getConnection() throws SQLException {
        return getDatabaseMetaData().getConnection();
    }

    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable)
                                                                                                        throws SQLException {
        return getDatabaseMetaData().getCrossReference(parentCatalog,
            parentSchema,
            parentTable,
            foreignCatalog,
            foreignSchema,
            foreignTable);
    }

    public int getDatabaseMajorVersion() throws SQLException {
        return getDatabaseMetaData().getDatabaseMajorVersion();
    }

    public int getDatabaseMinorVersion() throws SQLException {
        return getDatabaseMetaData().getDatabaseMinorVersion();
    }

    public String getDatabaseProductName() throws SQLException {
        return getDatabaseMetaData().getDatabaseProductName();
    }

    public String getDatabaseProductVersion() throws SQLException {
        return getDatabaseMetaData().getDatabaseProductVersion();
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        return getDatabaseMetaData().getDefaultTransactionIsolation();
    }

    public int getDriverMajorVersion() {
        try {
            return getDatabaseMetaData().getDriverMajorVersion();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int getDriverMinorVersion() {
        try {
            return getDatabaseMetaData().getDriverMinorVersion();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getDriverName() throws SQLException {
        return getDatabaseMetaData().getDriverName();
    }

    public String getDriverVersion() throws SQLException {
        return getDatabaseMetaData().getDriverVersion();
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return getDatabaseMetaData().getExportedKeys(catalog, schema, table);
    }

    public String getExtraNameCharacters() throws SQLException {
        return getDatabaseMetaData().getExtraNameCharacters();
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        return getDatabaseMetaData().getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
    }

    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return getDatabaseMetaData().getFunctions(catalog, schemaPattern, functionNamePattern);
    }

    public String getIdentifierQuoteString() throws SQLException {
        return getDatabaseMetaData().getIdentifierQuoteString();
    }

    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return getDatabaseMetaData().getImportedKeys(catalog, schema, table);
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
                                                                                                                   throws SQLException {
        return getDatabaseMetaData().getIndexInfo(catalog, schema, table, unique, approximate);
    }

    public int getJDBCMajorVersion() throws SQLException {
        return getDatabaseMetaData().getJDBCMajorVersion();
    }

    public int getJDBCMinorVersion() throws SQLException {
        return getDatabaseMetaData().getJDBCMinorVersion();
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        return getDatabaseMetaData().getMaxBinaryLiteralLength();
    }

    public int getMaxCatalogNameLength() throws SQLException {
        return getDatabaseMetaData().getMaxCatalogNameLength();
    }

    public int getMaxCharLiteralLength() throws SQLException {
        return getDatabaseMetaData().getMaxCharLiteralLength();
    }

    public int getMaxColumnNameLength() throws SQLException {
        return getDatabaseMetaData().getMaxColumnNameLength();
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        return getDatabaseMetaData().getMaxColumnsInGroupBy();
    }

    public int getMaxColumnsInIndex() throws SQLException {
        return getDatabaseMetaData().getMaxColumnsInIndex();
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        return getDatabaseMetaData().getMaxColumnsInOrderBy();
    }

    public int getMaxColumnsInSelect() throws SQLException {
        return getDatabaseMetaData().getMaxColumnsInSelect();
    }

    public int getMaxColumnsInTable() throws SQLException {
        return getDatabaseMetaData().getMaxColumnsInTable();
    }

    public int getMaxConnections() throws SQLException {
        return getDatabaseMetaData().getMaxConnections();
    }

    public int getMaxCursorNameLength() throws SQLException {
        return getDatabaseMetaData().getMaxCursorNameLength();
    }

    public int getMaxIndexLength() throws SQLException {
        return getDatabaseMetaData().getMaxIndexLength();
    }

    public int getMaxProcedureNameLength() throws SQLException {
        return getDatabaseMetaData().getMaxProcedureNameLength();
    }

    public int getMaxRowSize() throws SQLException {
        return getDatabaseMetaData().getMaxRowSize();
    }

    public int getMaxSchemaNameLength() throws SQLException {
        return getDatabaseMetaData().getMaxSchemaNameLength();
    }

    public int getMaxStatementLength() throws SQLException {
        return getDatabaseMetaData().getMaxStatementLength();
    }

    public int getMaxStatements() throws SQLException {
        return getDatabaseMetaData().getMaxStatements();
    }

    public int getMaxTableNameLength() throws SQLException {
        return getDatabaseMetaData().getMaxTableNameLength();
    }

    public int getMaxTablesInSelect() throws SQLException {
        return getDatabaseMetaData().getMaxTablesInSelect();
    }

    public int getMaxUserNameLength() throws SQLException {
        return getDatabaseMetaData().getMaxUserNameLength();
    }

    public String getNumericFunctions() throws SQLException {
        return getDatabaseMetaData().getNumericFunctions();
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return getDatabaseMetaData().getPrimaryKeys(catalog, schema, table);
    }

    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        return getDatabaseMetaData().getProcedureColumns(catalog,
            schemaPattern,
            procedureNamePattern,
            columnNamePattern);
    }

    public String getProcedureTerm() throws SQLException {
        return getDatabaseMetaData().getProcedureTerm();
    }

    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
                                                                                                     throws SQLException {
        return getDatabaseMetaData().getProcedures(catalog, schemaPattern, procedureNamePattern);
    }

    public int getResultSetHoldability() throws SQLException {
        return getDatabaseMetaData().getResultSetHoldability();
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return getDatabaseMetaData().getRowIdLifetime();
    }

    public String getSQLKeywords() throws SQLException {
        return getDatabaseMetaData().getSQLKeywords();
    }

    public int getSQLStateType() throws SQLException {
        return getDatabaseMetaData().getSQLStateType();
    }

    public String getSchemaTerm() throws SQLException {
        return getDatabaseMetaData().getSchemaTerm();
    }

    public ResultSet getSchemas() throws SQLException {
        return getDatabaseMetaData().getSchemas();
    }

    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return getDatabaseMetaData().getSchemas(catalog, schemaPattern);
    }

    public String getSearchStringEscape() throws SQLException {
        return getDatabaseMetaData().getSearchStringEscape();
    }

    public String getStringFunctions() throws SQLException {
        return getDatabaseMetaData().getStringFunctions();
    }

    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return getDatabaseMetaData().getSuperTables(catalog, schemaPattern, tableNamePattern);
    }

    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return getDatabaseMetaData().getSuperTypes(catalog, schemaPattern, typeNamePattern);
    }

    public String getSystemFunctions() throws SQLException {
        return getDatabaseMetaData().getSystemFunctions();
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
                                                                                                      throws SQLException {
        return getDatabaseMetaData().getTablePrivileges(catalog, schemaPattern, tableNamePattern);
    }

    public ResultSet getTableTypes() throws SQLException {
        return getDatabaseMetaData().getTableTypes();
    }

    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
                                                                                                             throws SQLException {
        return getDatabaseMetaData().getTables(catalog, schemaPattern, tableNamePattern, types);
    }

    public String getTimeDateFunctions() throws SQLException {
        return getDatabaseMetaData().getTimeDateFunctions();
    }

    public ResultSet getTypeInfo() throws SQLException {
        return getDatabaseMetaData().getTypeInfo();
    }

    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
                                                                                                       throws SQLException {
        return getDatabaseMetaData().getUDTs(catalog, schemaPattern, typeNamePattern, types);
    }

    public String getURL() throws SQLException {
        return getDatabaseMetaData().getURL();
    }

    public String getUserName() throws SQLException {
        return getDatabaseMetaData().getUserName();
    }

    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return getDatabaseMetaData().getVersionColumns(catalog, schema, table);
    }

    public boolean insertsAreDetected(int type) throws SQLException {
        return getDatabaseMetaData().insertsAreDetected(type);
    }

    public boolean isCatalogAtStart() throws SQLException {
        return getDatabaseMetaData().isCatalogAtStart();
    }

    public boolean isReadOnly() throws SQLException {
        return getDatabaseMetaData().isReadOnly();
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return getDatabaseMetaData().isWrapperFor(arg0);
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        return getDatabaseMetaData().locatorsUpdateCopy();
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        return getDatabaseMetaData().nullPlusNonNullIsNull();
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        return getDatabaseMetaData().nullsAreSortedAtEnd();
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        return getDatabaseMetaData().nullsAreSortedAtStart();
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        return getDatabaseMetaData().nullsAreSortedHigh();
    }

    public boolean nullsAreSortedLow() throws SQLException {
        return getDatabaseMetaData().nullsAreSortedLow();
    }

    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return getDatabaseMetaData().othersDeletesAreVisible(type);
    }

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return getDatabaseMetaData().othersInsertsAreVisible(type);
    }

    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return getDatabaseMetaData().othersUpdatesAreVisible(type);
    }

    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return getDatabaseMetaData().ownDeletesAreVisible(type);
    }

    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return getDatabaseMetaData().ownInsertsAreVisible(type);
    }

    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return getDatabaseMetaData().ownUpdatesAreVisible(type);
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return getDatabaseMetaData().storesLowerCaseIdentifiers();
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return getDatabaseMetaData().storesLowerCaseQuotedIdentifiers();
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return getDatabaseMetaData().storesMixedCaseIdentifiers();
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return getDatabaseMetaData().storesMixedCaseQuotedIdentifiers();
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return getDatabaseMetaData().storesUpperCaseIdentifiers();
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return getDatabaseMetaData().storesUpperCaseQuotedIdentifiers();
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return getDatabaseMetaData().supportsANSI92EntryLevelSQL();
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        return getDatabaseMetaData().supportsANSI92FullSQL();
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return getDatabaseMetaData().supportsANSI92IntermediateSQL();
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return getDatabaseMetaData().supportsAlterTableWithAddColumn();
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return getDatabaseMetaData().supportsAlterTableWithDropColumn();
    }

    public boolean supportsBatchUpdates() throws SQLException {
        return getDatabaseMetaData().supportsBatchUpdates();
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return getDatabaseMetaData().supportsCatalogsInDataManipulation();
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return getDatabaseMetaData().supportsCatalogsInIndexDefinitions();
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return getDatabaseMetaData().supportsCatalogsInPrivilegeDefinitions();
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return getDatabaseMetaData().supportsCatalogsInProcedureCalls();
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return getDatabaseMetaData().supportsCatalogsInTableDefinitions();
    }

    public boolean supportsColumnAliasing() throws SQLException {
        return getDatabaseMetaData().supportsColumnAliasing();
    }

    public boolean supportsConvert() throws SQLException {
        return getDatabaseMetaData().supportsConvert();
    }

    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return getDatabaseMetaData().supportsConvert(fromType, toType);
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        return getDatabaseMetaData().supportsCoreSQLGrammar();
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return getDatabaseMetaData().supportsCorrelatedSubqueries();
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return getDatabaseMetaData().supportsDataDefinitionAndDataManipulationTransactions();
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return getDatabaseMetaData().supportsDataManipulationTransactionsOnly();
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return getDatabaseMetaData().supportsDifferentTableCorrelationNames();
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return getDatabaseMetaData().supportsExpressionsInOrderBy();
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return getDatabaseMetaData().supportsExtendedSQLGrammar();
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        return getDatabaseMetaData().supportsFullOuterJoins();
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        return getDatabaseMetaData().supportsGetGeneratedKeys();
    }

    public boolean supportsGroupBy() throws SQLException {
        return getDatabaseMetaData().supportsGroupBy();
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return getDatabaseMetaData().supportsGroupByBeyondSelect();
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        return getDatabaseMetaData().supportsGroupByUnrelated();
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return getDatabaseMetaData().supportsIntegrityEnhancementFacility();
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        return getDatabaseMetaData().supportsLikeEscapeClause();
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        return getDatabaseMetaData().supportsLimitedOuterJoins();
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return getDatabaseMetaData().supportsMinimumSQLGrammar();
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return getDatabaseMetaData().supportsMixedCaseIdentifiers();
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return getDatabaseMetaData().supportsMixedCaseQuotedIdentifiers();
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        return getDatabaseMetaData().supportsMultipleOpenResults();
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        return getDatabaseMetaData().supportsMultipleResultSets();
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        return getDatabaseMetaData().supportsMultipleTransactions();
    }

    public boolean supportsNamedParameters() throws SQLException {
        return getDatabaseMetaData().supportsNamedParameters();
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        return getDatabaseMetaData().supportsNonNullableColumns();
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return getDatabaseMetaData().supportsOpenCursorsAcrossCommit();
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return getDatabaseMetaData().supportsOpenCursorsAcrossRollback();
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return getDatabaseMetaData().supportsOpenStatementsAcrossCommit();
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return getDatabaseMetaData().supportsOpenStatementsAcrossRollback();
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        return getDatabaseMetaData().supportsOrderByUnrelated();
    }

    public boolean supportsOuterJoins() throws SQLException {
        return getDatabaseMetaData().supportsOuterJoins();
    }

    public boolean supportsPositionedDelete() throws SQLException {
        return getDatabaseMetaData().supportsPositionedDelete();
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        return getDatabaseMetaData().supportsPositionedUpdate();
    }

    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return getDatabaseMetaData().supportsResultSetConcurrency(type, concurrency);
    }

    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return getDatabaseMetaData().supportsResultSetHoldability(holdability);
    }

    public boolean supportsResultSetType(int type) throws SQLException {
        return getDatabaseMetaData().supportsResultSetType(type);
    }

    public boolean supportsSavepoints() throws SQLException {
        return getDatabaseMetaData().supportsSavepoints();
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return getDatabaseMetaData().supportsSchemasInDataManipulation();
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return getDatabaseMetaData().supportsSchemasInIndexDefinitions();
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return getDatabaseMetaData().supportsSchemasInPrivilegeDefinitions();
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return getDatabaseMetaData().supportsSchemasInProcedureCalls();
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return getDatabaseMetaData().supportsSchemasInTableDefinitions();
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        return getDatabaseMetaData().supportsSelectForUpdate();
    }

    public boolean supportsStatementPooling() throws SQLException {
        return getDatabaseMetaData().supportsStatementPooling();
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return getDatabaseMetaData().supportsStoredFunctionsUsingCallSyntax();
    }

    public boolean supportsStoredProcedures() throws SQLException {
        return getDatabaseMetaData().supportsStoredProcedures();
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return getDatabaseMetaData().supportsSubqueriesInComparisons();
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        return getDatabaseMetaData().supportsSubqueriesInExists();
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        return getDatabaseMetaData().supportsSubqueriesInIns();
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return getDatabaseMetaData().supportsSubqueriesInQuantifieds();
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        return getDatabaseMetaData().supportsTableCorrelationNames();
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return getDatabaseMetaData().supportsTransactionIsolationLevel(level);
    }

    public boolean supportsTransactions() throws SQLException {
        return getDatabaseMetaData().supportsTransactions();
    }

    public boolean supportsUnion() throws SQLException {
        return getDatabaseMetaData().supportsUnion();
    }

    public boolean supportsUnionAll() throws SQLException {
        return getDatabaseMetaData().supportsUnionAll();
    }

    public <T> T unwrap(Class<T> arg0) throws SQLException {
        return getDatabaseMetaData().unwrap(arg0);
    }

    public boolean updatesAreDetected(int type) throws SQLException {
        return getDatabaseMetaData().updatesAreDetected(type);
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        return getDatabaseMetaData().usesLocalFilePerTable();
    }

    public boolean usesLocalFiles() throws SQLException {
        return getDatabaseMetaData().usesLocalFiles();
    }

}
