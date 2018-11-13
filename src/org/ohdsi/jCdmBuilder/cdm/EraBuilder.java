package org.ohdsi.jCdmBuilder.cdm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.DbSettings;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.ReadTextFile;

public class EraBuilder {
	public static int	DRUG_ERA		= 1;
	public static int	CONDITION_ERA	= 2;

	public static int	VERSION_4		= 4;
	public static int	VERSION_5		= 5;
	public static int	VERSION_6		= 6;

	public static void buildEra(DbSettings dbSettings, int cdmVersion, String sourceFolder, int domain) {
		if (domain == DRUG_ERA)
			StringUtilities.outputWithTime("Constructing drug eras");
		else
			StringUtilities.outputWithTime("Constructing condition eras");

		String resourceName = null;
		if (cdmVersion == VERSION_4 && domain == DRUG_ERA)
			resourceName = "drugEraV4.sql";
		if (cdmVersion == VERSION_5 && domain == DRUG_ERA)
			resourceName = "drugEraV5.sql";
		if (cdmVersion == VERSION_6 && domain == DRUG_ERA)
			resourceName = "drugEraV6.sql";
		if (cdmVersion == VERSION_4 && domain == CONDITION_ERA)
			resourceName = "conditionEraV4.sql";
		if (cdmVersion == VERSION_5 && domain == CONDITION_ERA)
			resourceName = "conditionEraV5.sql";
		if (cdmVersion == VERSION_6 && domain == CONDITION_ERA)
			resourceName = "conditionEraV6.sql";

		InputStream resourceStream = null;
		if (sourceFolder != null) {
			File localFile = new File(sourceFolder + resourceName);
			if (localFile.exists()) {
				if (localFile.canRead()) {
					try {
						resourceStream = new FileInputStream(localFile);
						System.out.println("Using local definition: " + resourceName);
					} catch (FileNotFoundException e) {
						throw new RuntimeException("ERROR opening file: " + sourceFolder + resourceName);
					}
				}
				else {
					throw new RuntimeException("ERROR reading file: " + sourceFolder + resourceName);
				}
			}
		}
		
		if (resourceStream == null) {
			resourceStream = EraBuilder.class.getResourceAsStream(resourceName);
		}
		
		String sql = "";
		for (String line : new ReadTextFile(resourceStream))
			sql = sql + line + "\n";

		String dbms = "sql server";
		if (dbSettings.dbType == DbType.ORACLE)
			dbms = "oracle";
		else if (dbSettings.dbType == DbType.POSTGRESQL)
			dbms = "postgresql";

		sql = SqlTranslate.translateSql(sql, "sql server", dbms);
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.use(dbSettings.database);
		connection.execute(sql);
		StringUtilities.outputWithTime("Finished constructing eras");
	}
}
