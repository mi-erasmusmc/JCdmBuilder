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
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JFrame;
import javax.swing.JOptionPane;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.DbSettings;
import org.ohdsi.jCdmBuilder.ErrorReport;
import org.ohdsi.jCdmBuilder.JCdmBuilderMain;
import org.ohdsi.jCdmBuilder.cdm.v5.CdmV5;
import org.ohdsi.jCdmBuilder.cdm.v6.CdmV6;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.ReadTextFile;

/**
 * Helper class for creating CDM V4 and V5 structures and indices
 * 
 * @author MSCHUEMI
 * 
 */
public class Cdm {
	
	public static final int CDM     = 0;
	public static final int RESULTS = 1;
	
	public static final String[] availableVersions = new String[] {
			"5.0.1",
			"5.3.0",
			"5.3.1",
			"5.4.0",
			"6.0.0"
	};
	
	
	public static CdmVx getCDM(String version) {
		CdmVx cdm = null;
		if (version.startsWith("5.")) {
			cdm = new CdmV5();
		}
		else if (version.startsWith("6.")) {
			cdm = new CdmV6();
		}
		return cdm;
	}
	
	
	public static void dropStructure(int currentStructure, DbSettings dbSettings, String version, String sourceFolder) {
		dropConstraints(currentStructure, dbSettings, version, sourceFolder);
		dropIndices(currentStructure, dbSettings, version, sourceFolder);
		dropTables(currentStructure, dbSettings, version, sourceFolder);
		dropSchema(currentStructure, dbSettings, version);
	}
	
	
	private static void dropConstraints(int currentStructure, DbSettings dbSettings, String version, String sourceFolder) {
		CdmVx cdm = getCDM(version);

		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(cdm.getClass());
		
		try {
			connection.use(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
			
			String schema = currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema;
			
			StringUtilities.outputWithTime("Deleting old " + (currentStructure == CDM ? "CDM" : "Results") + " indices if they exist");
			Map<String, List<String>> indices = connection.getIndices(schema);
			for (String tableName : indices.keySet()) {
				for (String index : indices.get(tableName)) {
					connection.dropIndexIfExists(schema, tableName, index);
				}
			}
			StringUtilities.outputWithTime("Done");

			StringUtilities.outputWithTime("Deleting old " + (currentStructure == CDM ? "CDM" : "Results") + " primary key constraints if they exist");
			Map<String, List<String>> primaryKeyConstraints = connection.getPrimaryKeyConstraints(schema);
			for (String tableName : primaryKeyConstraints.keySet()) {
				for (String primaryKeyConstraint : primaryKeyConstraints.get(tableName)) {
					connection.dropConstraintIfExists(schema, tableName, primaryKeyConstraint);
				}
			}
			StringUtilities.outputWithTime("Done");
		}
		catch (Exception e) {
			if (dbSettings.dbType != DbType.MSSQL) {
				throw e;
			}
		}
		
		connection.close();
	}
	
	
	private static void dropIndices(int currentStructure, DbSettings dbSettings, String version, String sourceFolder) {
		CdmVx cdm = getCDM(version);

		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(cdm.getClass());
		
		try {
			connection.use(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
			
			String schema = currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema;

			StringUtilities.outputWithTime("Deleting old " + (currentStructure == CDM ? "CDM" : "Results") + " foreign key constraints if they exist");
			Map<String, List<String>> foreignKeyConstraints = connection.getForeignKeyConstraints(schema);
			for (String tableName : foreignKeyConstraints.keySet()) {
				for (String foreignKeyConstraint : foreignKeyConstraints.get(tableName)) {
					connection.dropConstraintIfExists(schema, tableName, foreignKeyConstraint);
				}
			}
			StringUtilities.outputWithTime("Done");
		}
		catch (Exception e) {
			if (dbSettings.dbType != DbType.MSSQL) {
				throw e;
			}
		}
		
		connection.close();
	}
	
	
	private static void dropTables(int currentStructure, DbSettings dbSettings, String version, String sourceFolder) {
		CdmVx cdm = getCDM(version);

		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(cdm.getClass());
		
		try {
			connection.use(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
			
			StringUtilities.outputWithTime("Deleting old " + (currentStructure == CDM ? "CDM" : "Results") + " tables if they exist");
			for (String tableName : connection.getTableNames(currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema)) {
				connection.dropTableIfExists(dbSettings.cdmSchema, tableName);
			}
			StringUtilities.outputWithTime("Done");
		}
		catch (Exception e) {
			if (dbSettings.dbType != DbType.MSSQL) {
				throw e;
			}
		}
		
		connection.close();
	}
	
	
	private static void dropSchema(int currentStructure, DbSettings dbSettings, String version) {
		CdmVx cdm = getCDM(version);

		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(cdm.getClass());
		try {
			connection.use(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
			
			StringUtilities.outputWithTime("Deleting " + (currentStructure == CDM ? "CDM" : "Results") + " schema if it exists");
			connection.dropSchemaIfExists(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
			StringUtilities.outputWithTime("Done");
		}
		catch (Exception e) {
			if (dbSettings.dbType != DbType.MSSQL) {
				throw e;
			}
		}
		
		connection.close();
	}
	
	
	public static void createSchema(int currentStructure, DbSettings dbSettings, String version) {
		CdmVx cdm = getCDM(version);

		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(cdm.getClass());
		connection.use(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
		
		StringUtilities.outputWithTime("Creating " + (currentStructure == CDM ? "CDM" : "Results") + " schema");
		connection.createSchema(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
		
		connection.close();
		StringUtilities.outputWithTime("Done");
	}
	
	
	public static void createTables(int currentStructure, DbSettings dbSettings, String version, String sourceFolder, boolean idsToBigInt) {
		CdmVx cdm = getCDM(version);
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = currentStructure == CDM ? cdm.structureOracle() : cdm.resultsStructureOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = currentStructure == CDM ? cdm.structureMSSQL() : cdm.resultsStructureMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = currentStructure == CDM ? cdm.structurePostgreSQL() : cdm.resultsStructurePostgreSQL();
		}

		if (resourceName != null) {
			InputStream resourceStream = null;
			if (sourceFolder != null) {
				File localFile = new File(sourceFolder + "/Scripts/" + resourceName);
				if (localFile.exists()) {
					if (localFile.canRead()) {
						try {
							resourceStream = new FileInputStream(localFile);
							StringUtilities.outputWithTime("Using local definition: " + resourceName);
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
				resourceName = version + "/" + resourceName;
				URL resourceURL = cdm.getClass().getResource(resourceName);
				if (resourceURL != null) {
					resourceStream = cdm.getClass().getResourceAsStream(resourceName);
				}
				else {
					StringUtilities.outputWithTime("- " + (currentStructure == CDM ? "CDM" : "Results") + " data structure definition not found");
				}
			}

			if (resourceStream != null) {
				String schemaName = currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema;
				List<String> sqlLines = new ArrayList<>();
				for (String line : new ReadTextFile(resourceStream)) {
					if ((line.trim().length() > 0) && (!line.trim().substring(0, 1).equals("#"))) {
						while (line.contains("  ")) {
							line = line.replaceAll("  ", " ");
						}
						if (line.contains("CREATE TABLE ")) {
							line = line.replace("CREATE TABLE ", "CREATE TABLE " + schemaName + ".");
						}
						if (line.contains("INSERT INTO ")) {
							line = line.replace("INSERT INTO ", "INSERT INTO " + schemaName + ".");
						}
						sqlLines.add(line);
					}
				}
				
				RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
				connection.setContext(cdm.getClass());
				connection.use(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
							
				StringUtilities.outputWithTime("Creating " + (currentStructure == CDM ? "CDM" : "Results") + " data structure");
				if (idsToBigInt) {
					StringUtilities.outputWithTime("- Converting IDs to BIGINT");
					Pattern pattern = Pattern.compile("[^t]_id\\s+integer");
					for (int i = 0; i < sqlLines.size(); i++) {
						String line = sqlLines.get(i);
						Matcher matcher = pattern.matcher(line.toLowerCase());
						if (matcher.find())
							sqlLines.set(i, line.replace("INTEGER", "BIGINT"));
					}
				}
				
				if (currentStructure == Cdm.RESULTS) {
					StringUtilities.outputWithTime("- Adding CDM schema prefixes");
					for (int i = 0; i < sqlLines.size(); i++) {
						String line = sqlLines.get(i);
						// Check for reference to other table in other schema.
						if (line.contains(" REFERENCES ")) {
							String tableName = StringUtilities.findBetween(line, " REFERENCES ", "(");
							String searchTableName = tableName.trim().toLowerCase();
							if (!connection.getTableNames(dbSettings.resultsSchema).contains(searchTableName)) {
								String schemaPrefix = dbSettings.cdmSchema + ".";
								sqlLines.set(i, line.replace(" REFERENCES " + tableName + "(", " REFERENCES " + schemaPrefix + tableName.trim() + " ("));
							}
						}
					}
				}
				
				connection.execute(StringUtilities.join(sqlLines, "\n"));
				
				connection.close();
				StringUtilities.outputWithTime("Done");
			}
		}
	}
	
	public static void createIndices(int currentStructure, DbSettings dbSettings, String version, String sourceFolder) {
		CdmVx cdm = getCDM(version);
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = currentStructure == CDM ? cdm.indicesOracle() : cdm.resultsIndicesOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = currentStructure == CDM ? cdm.indicesMSSQL() : cdm.resultsIndicesMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = currentStructure == CDM ? cdm.indicesPostgreSQL() : cdm.resultsIndicesPostgreSQL();
		}
		
		if (resourceName != null) {
			InputStream resourceStream = null;
			if (sourceFolder != null) {
				File localFile = new File(sourceFolder + "/Scripts/" + resourceName);
				if (localFile.exists()) {
					if (localFile.canRead()) {
						try {
							resourceStream = new FileInputStream(localFile);
							StringUtilities.outputWithTime("Using local definition: " + resourceName);
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
				resourceName = version + "/" + resourceName;
				URL resourceURL = cdm.getClass().getResource(resourceName);
				if (resourceURL != null) {
					resourceStream = cdm.getClass().getResourceAsStream(resourceName);
				}
				else {
					StringUtilities.outputWithTime("- " + (currentStructure == CDM ? "CDM" : "Results") + " indices definition not found");
				}
			}

			if (resourceStream != null) {
				StringUtilities.outputWithTime("Creating " + (currentStructure == CDM ? "CDM" : "Results") + " indices");
				String schemaName = currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema;
				List<String> sqlLines = new ArrayList<>();
				for (String line : new ReadTextFile(resourceStream)) {
					if ((line.trim().length() > 0) && (!line.trim().substring(0, 1).equals("#"))) {
						while (line.contains("  ")) {
							line = line.replaceAll("  ", " ");
						}
						if (line.contains("ALTER TABLE ")) {
							line = line.replace("ALTER TABLE ", "ALTER TABLE " + schemaName + ".");
						}
						if (line.contains("CREATE ") && line.contains(" INDEX ") && line.contains(" ON ")) {
							line = line.replace(" ON ", " ON " + schemaName + ".");
						}
						sqlLines.add(line);
					}
				}
				
				RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
				connection.setContext(cdm.getClass());
				connection.use(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
				
				connection.execute(StringUtilities.join(sqlLines, "\n"));
				
				connection.close();
				StringUtilities.outputWithTime("Done");
			}
		}
	}
	
	
	public static void createConstraints(int currentStructure, DbSettings dbSettings, String version, String sourceFolder) {
		CdmVx cdm = getCDM(version);
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = currentStructure == CDM ? cdm.constraintsOracle() : cdm.resultsConstraintsOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = currentStructure == CDM ? cdm.constraintsMSSQL() : cdm.resultsConstraintsMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = currentStructure == CDM ? cdm.constraintsPostgreSQL() : cdm.resultsConstraintsPostgreSQL();
		}
		
		if (resourceName != null) {
			InputStream resourceStream = null;
			if (sourceFolder != null) {
				File localFile = new File(sourceFolder + "/Scripts/" + resourceName);
				if (localFile.exists()) {
					if (localFile.canRead()) {
						try {
							resourceStream = new FileInputStream(localFile);
							StringUtilities.outputWithTime("Using local definition: " + resourceName);
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
				resourceName = version + "/" + resourceName;
				URL resourceURL = cdm.getClass().getResource(resourceName);
				if (resourceURL != null) {
					resourceStream = cdm.getClass().getResourceAsStream(resourceName);
				}
				else {
					StringUtilities.outputWithTime("- " + (currentStructure == CDM ? "CDM" : "Results")+ " constraints definition not found");
				}
			}

			if (resourceStream != null) {
				StringUtilities.outputWithTime("Creating constraints");
				String schemaName = currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema;
				List<String> sqlLines = new ArrayList<>();
				for (String line : new ReadTextFile(resourceStream)) {
					if ((line.trim().length() > 0) && (!line.trim().substring(0, 1).equals("#"))) {
						while (line.contains("  ")) {
							line = line.replaceAll("  ", " ");
						}
						if (line.contains("ALTER TABLE ")) {
							line = line.replace("ALTER TABLE ", "ALTER TABLE " + schemaName + ".");
						}
						if (line.contains("REFERENCES ")) {
							line = line.replace("REFERENCES ", "REFERENCES " + schemaName + ".");
						}
						sqlLines.add(line);
					}
				}
				
				RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
				connection.setContext(cdm.getClass());
				connection.use(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
				
				connection.execute(StringUtilities.join(sqlLines, "\n"));
				
				connection.close();
				StringUtilities.outputWithTime("Done");
			}
		}
	}
	
	
	public static void createPatchTables(int currentStructure, DbSettings dbSettings, String version, String sourceFolder, boolean idsToBigInt) {
		CdmVx cdm = getCDM(version);
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = currentStructure == CDM ? cdm.structureOracle() : cdm.resultsStructureOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = currentStructure == CDM ? cdm.structureMSSQL() : cdm.resultsStructureMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = currentStructure == CDM ? cdm.structurePostgreSQL() : cdm.resultsStructurePostgreSQL();
		}

		if (resourceName != null) {
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
							StringUtilities.outputWithTime("Using local definition: " + resourceName);
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
				String schemaName = currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema;
				List<String> sqlLines = new ArrayList<>();
				for (String line : new ReadTextFile(resourceStream)) {
					if ((line.trim().length() > 0) && (!line.trim().substring(0, 1).equals("#"))) {
						while (line.contains("  ")) {
							line = line.replaceAll("  ", " ");
						}
						if (line.contains("DROP TABLE IF EXISTS ")) {
							line = line.replace("DROP TABLE IF EXISTS ", "DROP TABLE IF EXISTS " + schemaName + ".");
						}
						else if (line.contains("DROP TABLE ")) {
							line = line.replace("DROP TABLE ", "DROP TABLE " + schemaName + ".");
						}
						else if (line.contains("CREATE TABLE ")) {
							line = line.replace("CREATE TABLE ", "CREATE TABLE " + schemaName + ".");
						}
						sqlLines.add(line);
					}
				}

				RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
				connection.setContext(cdm.getClass());
				connection.use(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
				
				StringUtilities.outputWithTime("Patching " + (currentStructure == CDM ? "CDM" : "Results") + " data structure");
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
	}
	
	
	public static void createPatchIndices(int currentStructure, DbSettings dbSettings, String version, String sourceFolder) {
		CdmVx cdm = getCDM(version);
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = currentStructure == CDM ? cdm.indicesOracle() : cdm.resultsIndicesOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = currentStructure == CDM ? cdm.indicesMSSQL() : cdm.resultsIndicesMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = currentStructure == CDM ? cdm.indicesPostgreSQL() : cdm.resultsIndicesPostgreSQL();
		}
		
		if (resourceName != null) {
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
							StringUtilities.outputWithTime("Using local definition: " + resourceName);
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
				StringUtilities.outputWithTime("Patching " + (currentStructure == CDM ? "CDM" : "Results") + " indices");
				String schemaName = currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema;
				List<String> sqlLines = new ArrayList<>();
				for (String line : new ReadTextFile(resourceStream)) {
					if ((line.trim().length() > 0) && (!line.trim().substring(0, 1).equals("#"))) {
						while (line.contains("  ")) {
							line = line.replaceAll("  ", " ");
						}
						if (line.contains("ALTER TABLE ")) {
							line = line.replace("ALTER TABLE ", "ALTER TABLE " + schemaName + ".");
						}
						if (line.contains("CREATE ") && line.contains(" INDEX ") && line.contains(" ON ")) {
							line = line.replace(" ON ", " ON " + schemaName + ".");
						}
						sqlLines.add(line);
					}
				}
				
				RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
				connection.setContext(cdm.getClass());
				connection.use(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
				
				connection.execute(StringUtilities.join(sqlLines, "\n"));
				
				connection.close();
				StringUtilities.outputWithTime("Done");
			}
		}
	}
	
	
	public static void createPatchConstraints(int currentStructure, DbSettings dbSettings, String version, String sourceFolder) {
		CdmVx cdm = getCDM(version);
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = currentStructure == CDM ? cdm.constraintsOracle() : cdm.resultsConstraintsOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = currentStructure == CDM ? cdm.constraintsMSSQL() : cdm.resultsConstraintsMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = currentStructure == CDM ? cdm.constraintsPostgreSQL() : cdm.resultsConstraintsPostgreSQL();
		}

		if (resourceName != null) {
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
							StringUtilities.outputWithTime("Using local definition: " + resourceName);
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
				StringUtilities.outputWithTime("Patching " + (currentStructure == CDM ? "CDM" : "Results") + " constraints");
				String schemaName = currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema;
				List<String> sqlLines = new ArrayList<>();
				for (String line : new ReadTextFile(resourceStream)) {
					if ((line.trim().length() > 0) && (!line.trim().substring(0, 1).equals("#"))) {
						while (line.contains("  ")) {
							line = line.replaceAll("  ", " ");
						}
						if (line.contains("ALTER TABLE ")) {
							line = line.replace("ALTER TABLE ", "ALTER TABLE " + schemaName + ".");
						}
						if (line.contains("REFERENCES ")) {
							line = line.replace("REFERENCES ", "REFERENCES " + schemaName + ".");
						}
						sqlLines.add(line);
					}
				}
				
				RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
				connection.setContext(cdm.getClass());
				connection.use(currentStructure == CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
				
				connection.execute(StringUtilities.join(sqlLines, "\n"));
				
				connection.close();
				StringUtilities.outputWithTime("Done");
			}
		}
	}
	
	
	private static void handleError(Exception e, JFrame frame, String errorFolder, String item, boolean continueOnError) {
		JCdmBuilderMain.errors.add(item);
		System.err.println("Error: " + e.getMessage());
		String errorReportFilename = ErrorReport.generate(errorFolder, e, item);
		String message = "Error: " + e.getLocalizedMessage();
		message += "\nAn error report has been generated:\n" + errorReportFilename;
		System.out.println(message);
		if (!continueOnError) {
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error", JOptionPane.ERROR_MESSAGE);
		}
	}
}
