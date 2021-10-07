package org.ohdsi.jCdmBuilder.vocabulary;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.DbSettings;
import org.ohdsi.utilities.StringUtilities;

public class CopyVocabularyFromSchema {
	public void process(String vocabSchema, DbSettings dbSettings) {
		String resourceName = null;
		if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = "CopyVocabFromSchema - SQL Server.sql";
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = "CopyVocabFromSchema - PostgreSQL.sql";
		} else if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = "CopyVocabFromSchema - Oracle.sql";
			throw new RuntimeException("Copying vocabulary not implemented for Oracle. Contact JCDMBuilder maintainer if needed.");
		}
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.use(dbSettings.cdmSchema);
		connection.setContext(CopyVocabularyFromSchema.class);
		connection.setVerbose(true);
		StringUtilities.outputWithTime("Copying vocabulary tables from schema " + vocabSchema);
		connection.executeResource(resourceName, "@target_schema", dbSettings.cdmSchema, "@vocab_schema", vocabSchema);
		connection.close();
		StringUtilities.outputWithTime("Done");
	}
}
