/*******************************************************************************
 * Copyright 2017 Observational Health Data Sciences and Informatics
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.jCdmBuilder.cdm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.DbSettings;
import org.ohdsi.jCdmBuilder.cdm.v4.CdmV4;
import org.ohdsi.jCdmBuilder.cdm.v5.CdmV5;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.ReadTextFile;

/**
 * Helper class for creating CDM V4 and V5 structures and indices
 * 
 * @author MSCHUEMI
 * 
 */
public class Cdm {
	
	public static final int	VERSION_4	=   4;
	public static final int	VERSION_501	= 501;
	public static final int	VERSION_53	=  53;
	
	public static void createStructure(DbSettings dbSettings, int version, String sourceFolder, boolean idsToBigInt) {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else
			cdm = new CdmV5();
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = cdm.structureOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = cdm.structureMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = cdm.structurePostgreSQL();
		}

		InputStream resourceStream = null;
		if (sourceFolder != null) {
			File localFile = new File(sourceFolder + "/Scripts/" + resourceName);
			if (localFile.exists()) {
				if (localFile.canRead()) {
					try {
						resourceStream = new FileInputStream(localFile);
						System.out.println("Using local definition: " + resourceName);
					} catch (FileNotFoundException e) {
						throw new RuntimeException("ERROR opening file: " + sourceFolder + "/Scripts/" + resourceName);
					}
				}
				else {
					throw new RuntimeException("ERROR reading file: " + sourceFolder + "/Scripts/" + resourceName);
				}
			}
		}
		
		if (resourceStream == null) {
			resourceName = (version == VERSION_4 ? "" : (version == VERSION_501 ? "5.0.1/" : "5.3/")) + resourceName;
			resourceStream = cdm.getClass().getResourceAsStream(resourceName);
		}
		
		List<String> sqlLines = new ArrayList<>();
		for (String line : new ReadTextFile(resourceStream))
			sqlLines.add(line);

		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(cdm.getClass());
		connection.use(dbSettings.database);
		StringUtilities.outputWithTime("Deleting old tables if they exist");
		String currentCreate = "";
		for (String line : sqlLines) {
			currentCreate += line;
			if (currentCreate.contains("CREATE TABLE ")) {
				String tableName = StringUtilities.findBetween(currentCreate, "CREATE TABLE", "(").trim();
				if (tableName.length() != 0) {
					connection.dropTableIfExists(tableName);
					currentCreate = "";
				}
			}
			else {
				currentCreate = "";
			}
		}
		
		StringUtilities.outputWithTime("Creating CDM data structure");
		if (idsToBigInt) {
			System.out.println("- Converting IDs to BIGINT");
			Pattern pattern = Pattern.compile("[^t]_id\\s+integer");
			for (int i = 0; i < sqlLines.size(); i++) {
				String line = sqlLines.get(i);
				Matcher matcher = pattern.matcher(line.toLowerCase());
				if (matcher.find())
					sqlLines.set(i, line.replace("INTEGER", "BIGINT"));
			}
		}
		connection.execute(StringUtilities.join(sqlLines, "\n"));
		
		connection.close();
		StringUtilities.outputWithTime("Done");
	}
	
	public static void patchStructure(DbSettings dbSettings, int version, String sourceFolder, boolean idsToBigInt) {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else
			cdm = new CdmV5();
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = cdm.structureOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = cdm.structureMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = cdm.structurePostgreSQL();
		}

		int lastDotPosition = resourceName.lastIndexOf(".", resourceName.length());
		if (lastDotPosition == -1) {
			resourceName = resourceName + " - Patch";
		}
		else {
			resourceName = resourceName.substring(0, lastDotPosition) + " - Patch" + resourceName.substring(lastDotPosition);
		}

		InputStream resourceStream = null;
		if (sourceFolder != null) {
			File localFile = new File(sourceFolder + "/Scripts/" + resourceName);
			if (localFile.exists()) {
				if (localFile.canRead()) {
					try {
						resourceStream = new FileInputStream(localFile);
						System.out.println("Using local definition: " + resourceName);
					} catch (FileNotFoundException e) {
						throw new RuntimeException("ERROR opening file: " + sourceFolder + "/Scripts/" + resourceName);
					}
				}
				else {
					throw new RuntimeException("ERROR reading file: " + sourceFolder + "/Scripts/" + resourceName);
				}
			}
		}
		
		if (resourceStream != null) {
			List<String> sqlLines = new ArrayList<>();
			for (String line : new ReadTextFile(resourceStream))
				sqlLines.add(line);

			RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
			connection.setContext(cdm.getClass());
			connection.use(dbSettings.database);
			
			StringUtilities.outputWithTime("Patching CDM data structure");
			if (idsToBigInt) {
				System.out.println("- Converting IDs to BIGINT");
				Pattern pattern = Pattern.compile("[^t]_id\\s+integer");
				for (int i = 0; i < sqlLines.size(); i++) {
					String line = sqlLines.get(i);
					Matcher matcher = pattern.matcher(line.toLowerCase());
					if (matcher.find())
						sqlLines.set(i, line.replace("INTEGER", "BIGINT"));
				}
			}
			connection.execute(StringUtilities.join(sqlLines, "\n"));
			
			connection.close();
			StringUtilities.outputWithTime("Done");
		}
		
	}
	
	public static void createIndices(DbSettings dbSettings, int version, String sourceFolder) {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else
			cdm = new CdmV5();
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = cdm.indexesOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = cdm.indexesMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = cdm.indexesPostgreSQL();
		}
		
		boolean localDefinition = false;
		if (sourceFolder != null) {
			File localFile = new File(sourceFolder + "/Scripts/" + resourceName);
			if (localFile.exists()) {
				if (localFile.canRead()) {
					System.out.println("Using local definition: " + resourceName);
					resourceName = sourceFolder + "/Scripts/" + resourceName;
					localDefinition = true;
				}
				else {
					throw new RuntimeException("ERROR reading file: " + sourceFolder + "/Scripts/" + resourceName);
				}
			}
		}
		
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(cdm.getClass());
		
		StringUtilities.outputWithTime("Creating CDM indices");
		connection.use(dbSettings.database);
		if (localDefinition) {
			connection.executeLocalFile(resourceName);
		}
		else {
			resourceName = (version == VERSION_4 ? "" : (version == VERSION_501 ? "5.0.1/" : "5.3/")) + resourceName;
			connection.executeResource(resourceName);
		}
		
		connection.close();
		StringUtilities.outputWithTime("Done");
	}
	
	public static void patchIndices(DbSettings dbSettings, int version, String sourceFolder) {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else
			cdm = new CdmV5();
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = cdm.indexesOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = cdm.indexesMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = cdm.indexesPostgreSQL();
		}

		int lastDotPosition = resourceName.lastIndexOf(".", resourceName.length());
		if (lastDotPosition == -1) {
			resourceName = resourceName + " - Patch";
		}
		else {
			resourceName = resourceName.substring(0, lastDotPosition) + " - Patch" + resourceName.substring(lastDotPosition);
		}
		
		boolean patchFound = false;
		if (sourceFolder != null) {
			File localFile = new File(sourceFolder + "/Scripts/" + resourceName);
			if (localFile.exists()) {
				if (localFile.canRead()) {
					System.out.println("Using local definition: " + resourceName);
					resourceName = sourceFolder + "/Scripts/" + resourceName;
					patchFound = true;
				}
				else {
					throw new RuntimeException("ERROR reading file: " + sourceFolder + "/Scripts/" + resourceName);
				}
			}
		}
		
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(cdm.getClass());
		
		StringUtilities.outputWithTime("Patching CDM indices");
		connection.use(dbSettings.database);
		if (patchFound) {
			connection.executeLocalFile(resourceName);
		}
		
		connection.close();
		StringUtilities.outputWithTime("Done");
	}
}
