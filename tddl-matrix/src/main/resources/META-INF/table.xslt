<xsl:stylesheet
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  version="1.0">

    <!-- Template to add a default namespace to a document -->
    <!-- Elements without a namespace are "moved" to default namespace -->
    <!-- Elements with a namespace are copied as such -->

  <!-- string for default namespace uri and schema location -->
  <xsl:variable name="ns" select="'namespaceURL'"/>
  <xsl:variable name="schemaLoc" select="'namespaceURL pathToMySchema.xsd'"/>

    <!-- template for root element -->
    <!-- adds default namespace and schema location -->
  <xsl:template match="/*" priority="1">
    <xsl:element name="{local-name()}" namespace="{$ns}">
      <xsl:attribute name="xsi:schemaLocation"
        namespace="http://www.w3.org/2001/XMLSchema-instance">
        <xsl:value-of select="$schemaLoc"/>
        </xsl:attribute>
      <xsl:apply-templates select="@* | node()"/>
    </xsl:element>
  </xsl:template>

    <!--template for elements without a namespace -->
  <xsl:template match="*[namespace-uri() = '']">
    <xsl:element name="{local-name()}" namespace="{$ns}">
      <xsl:apply-templates select="@* | node()"/>
    </xsl:element>
  </xsl:template>

    <!--template for elements with a namespace -->
  <xsl:template match="*[not(namespace-uri() = '')]">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()"/>
    </xsl:copy>
  </xsl:template>

    <!--template to copy attributes, text, PIs and comments -->
  <xsl:template match="@* | node()">
    <xsl:copy>
      <xsl:apply-templates select="@* | node()"/>
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>