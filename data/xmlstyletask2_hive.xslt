<?xml version="1.0"?>
<xsl:stylesheet version="1.0"
		xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
        xmlns:date="http://exslt.org/dates-and-times">
<xsl:output method="text" omit-xml-declaration="yes" indent="no"/>
<xsl:template match="/">
<xsl:for-each select="posts/row">
<xsl:variable name="CreationDateTime" select="@CreationDate" />
<xsl:variable name="CreationDate" select="date:date($CreationDateTime)" />
<xsl:variable name="CreationTime" select="date:time($CreationDateTime)" />
<xsl:value-of select="concat(@Id,',',@PostTypeId,',',@ParentId,',',@AcceptedAnswerId,',',$CreationDate,' ',$CreationTime,'&#xA;')"/>
</xsl:for-each>
</xsl:template>
</xsl:stylesheet>
