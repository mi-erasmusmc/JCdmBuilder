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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JFrame;
import javax.swing.JOptionPane;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.DbSettings;
import org.ohdsi.jCdmBuilder.ErrorReport;
import org.ohdsi.jCdmBuilder.JCdmBuilderMain;
import org.ohdsi.jCdmBuilder.cdm.v4.CdmV4;
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
	
	public static final int	VERSION_4	= 400;
	public static final int	VERSION_501	= 501;
	public static final int	VERSION_530	= 530;
	public static final int	VERSION_531	= 531;
	public static final int	VERSION_600	= 600;
	
	public static final int CDM     = 0;
	public static final int RESULTS = 1;
	
	
	public static void dropStructure(int currentStructure, DbSettings dbSettings, int version, String sourceFolder) {
		dropConstraints(currentStructure, dbSettings, version, sourceFolder);
		dropTables(currentStructure, dbSettings, version, sourceFolder);
		dropSchema(currentStructure, dbSettings, version);
	}
	
	public static void dropConstraints(int currentStructure, DbSettings dbSettings, int version, String sourceFolder) {
		if (currentStructure == CDM) {
			CdmVx cdm;
			if (version == VERSION_4)
				cdm = new CdmV4();
			else if ((version == VERSION_501) || (version == VERSION_530) || (version == VERSION_531))
				cdm = new CdmV5();
			else
				cdm = new CdmV6();
			
			String resourceName = null;
			if (dbSettings.dbType == DbType.ORACLE) {
				resourceName = cdm.constraintsOracle();
			} else if (dbSettings.dbType == DbType.MSSQL) {
				resourceName = cdm.constraintsMSSQL();
			} else if (dbSettings.dbType == DbType.POSTGRESQL) {
				resourceName = cdm.constraintsPostgreSQL();
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
					resourceName = (version == VERSION_4 ? "" : (version == VERSION_501 ? "5.0.1/" : (version == VERSION_530 ? "5.3.0/" : (version == VERSION_531 ? "5.3.1/" : "6.0.0/")))) + resourceName;
					URL resourceURL = cdm.getClass().getResource(resourceName);
					if (resourceURL != null) {
						resourceStream = cdm.getClass().getResourceAsStream(resourceName);
					}
					else {
						StringUtilities.outputWithTime("- " + (currentStructure == CDM ? "CDM" : "Results") + " constraints definition not found");
					}
				}
				
				List<String> sqlLines = new ArrayList<>();
				for (String line : new ReadTextFile(resourceStream)) {
					if ((line.trim().length() > 0) && (!line.trim().substring(0, 1).equals("#"))) {
						sqlLines.add(line);
					}
				}

				RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
				connection.setContext(cdm.getClass());
				connection.use(currentStructure == CDM ? dbSettings.database : dbSettings.resultsDatabase);
				
				StringUtilities.outputWithTime("Deleting old " + (currentStructure == CDM ? "CDM" : "Results") + " constraints if they exist");
				String currentConstraint = "";
				for (String line : sqlLines) {
					currentConstraint += line;
					if (currentConstraint.contains("ALTER TABLE ") && currentConstraint.contains(" ADD CONSTAINT ") && currentConstraint.contains(" FOREIGN KEY ")) {
						String tableName = StringUtilities.findBetween(currentConstraint, "ALTER TABLE ", " ADD CONSTAINT ").trim();
						String constraintName = StringUtilities.findBetween(currentConstraint, " ADD CONSTAINT ", " FOREIGN KEY ").trim();
						if (tableName.length() != 0) {
							connection.dropConstraintIfExists(tableName, constraintName);
							currentConstraint = "";
						}
					}
					else {
						currentConstraint = "";
					}
				}
				
				connection.close();
				StringUtilities.outputWithTime("Done");
			}
		}
	}
	
	public static void dropTables(int currentStructure, DbSettings dbSettings, int version, String sourceFolder) {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else if ((version == VERSION_501) || (version == VERSION_530) || (version == VERSION_531))
			cdm = new CdmV5();
		else
			cdm = new CdmV6();
		
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
				resourceName = (version == VERSION_4 ? "" : (version == VERSION_501 ? "5.0.1/" : (version == VERSION_530 ? "5.3.0/" : (version == VERSION_531 ? "5.3.1/" : "6.0.0/")))) + resourceName;
				URL resourceURL = cdm.getClass().getResource(resourceName);
				if (resourceURL != null) {
					resourceStream = cdm.getClass().getResourceAsStream(resourceName);
				}
				else {
					StringUtilities.outputWithTime("- " + (currentStructure == CDM ? "CDM" : "Results") + " data structure definition not found");
				}
			}
			
			List<String> sqlLines = new ArrayList<>();
			for (String line : new ReadTextFile(resourceStream)) {
				if ((line.trim().length() > 0) && (!line.trim().substring(0, 1).equals("#"))) {
					sqlLines.add(line);
				}
			}

			RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
			connection.setContext(cdm.getClass());
			connection.use(currentStructure == CDM ? dbSettings.database : dbSettings.resultsDatabase);
			
			StringUtilities.outputWithTime("Deleting old " + (currentStructure == CDM ? "CDM" : "Results") + " tables if they exist");
			for (String tableName : connection.getTableNames(currentStructure == Cdm.CDM ? dbSettings.database : dbSettings.resultsDatabase)) {
				connection.dropTableIfExists(tableName);
			}

			connection.close();
			StringUtilities.outputWithTime("Done");
		}
	}
	
	public static void dropSchema(int currentStructure, DbSettings dbSettings, int version) {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else if ((version == VERSION_501) || (version == VERSION_530) || (version == VERSION_531))
			cdm = new CdmV5();
		else
			cdm = new CdmV6();

		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(cdm.getClass());
		connection.use(currentStructure == CDM ? dbSettings.database : dbSettings.resultsDatabase);
		
		StringUtilities.outputWithTime("Deleting " + (currentStructure == CDM ? "CDM" : "Results") + " schema if it exists");
		connection.dropSchemaIfExists(currentStructure == CDM ? dbSettings.database : dbSettings.resultsDatabase);
		
		connection.close();
		StringUtilities.outputWithTime("Done");
	}
	
	public static void createSchema(int currentStructure, DbSettings dbSettings, int version) {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else if ((version == VERSION_501) || (version == VERSION_530) || (version == VERSION_531))
			cdm = new CdmV5();
		else
			cdm = new CdmV6();

		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.setContext(cdm.getClass());
		connection.use(currentStructure == CDM ? dbSettings.database : dbSettings.resultsDatabase);
		
		StringUtilities.outputWithTime("Creating " + (currentStructure == CDM ? "CDM" : "Results") + " schema");
		connection.createSchema(currentStructure == CDM ? dbSettings.database : dbSettings.resultsDatabase);
		
		connection.close();
		StringUtilities.outputWithTime("Done");
	}
	
	public static void createStructure(int currentStructure, DbSettings dbSettings, int version, String sourceFolder, boolean idsToBigInt) {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else if ((version == VERSION_501) || (version == VERSION_530) || (version == VERSION_531))
			cdm = new CdmV5();
		else
			cdm = new CdmV6();
		
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
				resourceName = (version == VERSION_4 ? "" : (version == VERSION_501 ? "5.0.1/" : (version == VERSION_530 ? "5.3.0/" : (version == VERSION_531 ? "5.3.1/" : "6.0.0/")))) + resourceName;
				URL resourceURL = cdm.getClass().getResource(resourceName);
				if (resourceURL != null) {
					resourceStream = cdm.getClass().getResourceAsStream(resourceName);
				}
				else {
					StringUtilities.outputWithTime("- " + (currentStructure == CDM ? "CDM" : "Results")+ " data structure definition not found");
				}
			}
			
			List<String> sqlLines = new ArrayList<>();
			for (String line : new ReadTextFile(resourceStream)) {
				if ((line.trim().length() > 0) && (!line.trim().substring(0, 1).equals("#"))) {
					sqlLines.add(line);
				}
			}
			
			RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
			connection.setContext(cdm.getClass());
			connection.use(currentStructure == CDM ? dbSettings.database : dbSettings.resultsDatabase);
						
			StringUtilities.outputWithTime("Creating " + (currentStructure == CDM ? "CDM" : "Results")+ " data structure");
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
						if (!connection.getTableNames(dbSettings.resultsDatabase).contains(searchTableName)) {
							String schemaPrefix = dbSettings.database + ".";
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
	
	public static void dropPatchStructure(int currentStructure, DbSettings dbSettings, int version, String sourceFolder) {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else if ((version == VERSION_501) || (version == VERSION_530) || (version == VERSION_531))
			cdm = new CdmV5();
		else
			cdm = new CdmV6();
		
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
				List<String> sqlLines = new ArrayList<>();
				for (String line : new ReadTextFile(resourceStream)) {
					if ((line.trim().length() > 0) && (!line.trim().substring(0, 1).equals("#"))) {
						sqlLines.add(line);
					}
				}

				RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
				connection.setContext(cdm.getClass());
				connection.use(currentStructure == CDM ? dbSettings.database : dbSettings.resultsDatabase);
				
				StringUtilities.outputWithTime("Deleting old Patch tables if they exist");
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
				
				connection.close();
				StringUtilities.outputWithTime("Done");
			}
		}
	}
	
	public static void patchStructure(int currentStructure, DbSettings dbSettings, int version, String sourceFolder, boolean idsToBigInt) {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else if ((version == VERSION_501) || (version == VERSION_530) || (version == VERSION_531))
			cdm = new CdmV5();
		else
			cdm = new CdmV6();
		
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
				List<String> sqlLines = new ArrayList<>();
				for (String line : new ReadTextFile(resourceStream)) {
					if ((line.trim().length() > 0) && (!line.trim().substring(0, 1).equals("#"))) {
						sqlLines.add(line);
					}
				}

				RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
				connection.setContext(cdm.getClass());
				connection.use(currentStructure == CDM ? dbSettings.database : dbSettings.resultsDatabase);
				
				StringUtilities.outputWithTime("Patching " + (currentStructure == CDM ? "CDM" : "Results")+ " data structure");
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
	
	public static void createIndices(int currentStructure, DbSettings dbSettings, int version, String sourceFolder, JFrame frame, String errorFolder, boolean continueOnError) throws Exception {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else if ((version == VERSION_501) || (version == VERSION_530) || (version == VERSION_531))
			cdm = new CdmV5();
		else
			cdm = new CdmV6();
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = currentStructure == CDM ? cdm.indexesOracle() : cdm.resultsIndexesOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = currentStructure == CDM ? cdm.indexesMSSQL() : cdm.resultsIndexesMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = currentStructure == CDM ? cdm.indexesPostgreSQL() : cdm.resultsIndexesPostgreSQL();
		}
		
		RichConnection connection = null;
		if (resourceName != null) {
			try {
				boolean localDefinition = false;
				if (sourceFolder != null) {
					File localFile = new File(sourceFolder + "/Scripts/" + resourceName);
					if (localFile.exists()) {
						if (localFile.canRead()) {
							StringUtilities.outputWithTime("Using local definition: " + resourceName);
							resourceName = sourceFolder + "/Scripts/" + resourceName;
							localDefinition = true;
						}
						else {
							throw new RuntimeException("ERROR reading file: " + sourceFolder + "/Scripts/" + resourceName);
						}
					}
				}
				
				connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
				connection.setContext(cdm.getClass());
				
				StringUtilities.outputWithTime("Creating " + (currentStructure == CDM ? "CDM" : "Results")+ " indices");
				connection.use(currentStructure == CDM ? dbSettings.database : dbSettings.resultsDatabase);
				if (localDefinition) {
					connection.executeLocalFile(resourceName);
				}
				else {
					resourceName = (version == VERSION_4 ? "" : (version == VERSION_501 ? "5.0.1/" : (version == VERSION_530 ? "5.3.0/" : (version == VERSION_531 ? "5.3.1/" : "6.0.0/")))) + resourceName;
					URL resourceURL = cdm.getClass().getResource(resourceName);
					if (resourceURL != null) {
						connection.executeResource(resourceName);
					}
					else {
						StringUtilities.outputWithTime("- " + (currentStructure == CDM ? "CDM" : "Results")+ " indices defintion not found.");
					}
				}
			} catch (Exception e) {
				handleError(e, frame, errorFolder, "Creating Indices", continueOnError);
				if (!continueOnError && (JOptionPane.showConfirmDialog(frame, "Do you want to continue?","Continue?",JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION)) {
					throw new Exception("NO ERROR");
				}
			} finally {
				if (connection != null) {
					connection.close();
					StringUtilities.outputWithTime("Done");
				}
			}
		}
	}
	
	public static void patchIndices(int currentStructure, DbSettings dbSettings, int version, String sourceFolder, JFrame frame, String errorFolder, boolean continueOnError) throws Exception {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else if ((version == VERSION_501) || (version == VERSION_530) || (version == VERSION_531))
			cdm = new CdmV5();
		else
			cdm = new CdmV6();
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = currentStructure == CDM ? cdm.indexesOracle() : cdm.resultsIndexesOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = currentStructure == CDM ? cdm.indexesMSSQL() : cdm.resultsIndexesMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = currentStructure == CDM ? cdm.indexesPostgreSQL() : cdm.resultsIndexesPostgreSQL();
		}

		if (resourceName != null) {
			RichConnection connection = null;
			try {
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
							StringUtilities.outputWithTime("Using local definition: " + resourceName);
							resourceName = sourceFolder + "/Scripts/" + resourceName;
							patchFound = true;
						}
						else {
							throw new RuntimeException("ERROR reading file: " + sourceFolder + "/Scripts/" + resourceName);
						}
					}
				}
				
				connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
				connection.setContext(cdm.getClass());
				
				StringUtilities.outputWithTime("Patching " + (currentStructure == CDM ? "CDM" : "Results")+ " indices");
				connection.use(currentStructure == CDM ? dbSettings.database : dbSettings.resultsDatabase);
				if (patchFound) {
					connection.executeLocalFile(resourceName);
				}
			} catch (Exception e) {
				handleError(e, frame, errorFolder, "Patching Indices", continueOnError);
				if (!continueOnError && (JOptionPane.showConfirmDialog(frame, "Do you want to continue?","Continue?",JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION)) {
					throw new Exception("NO ERROR");
				}
			} finally {
				if (connection != null) {
					connection.close();
					StringUtilities.outputWithTime("Done");
				}
			}
		}
	}
	
	public static void createConstraints(int currentStructure, DbSettings dbSettings, int version, String sourceFolder, JFrame frame, String errorFolder, boolean continueOnError) throws Exception {
		CdmVx cdm;
		if (version == VERSION_4)
			cdm = new CdmV4();
		else if ((version == VERSION_501) || (version == VERSION_530) || (version == VERSION_531))
			cdm = new CdmV5();
		else
			cdm = new CdmV6();
		
		String resourceName = null;
		if (dbSettings.dbType == DbType.ORACLE) {
			resourceName = currentStructure == CDM ? cdm.constraintsOracle() : cdm.resultsConstraintsOracle();
		} else if (dbSettings.dbType == DbType.MSSQL) {
			resourceName = currentStructure == CDM ? cdm.constraintsMSSQL() : cdm.resultsConstraintsMSSQL();
		} else if (dbSettings.dbType == DbType.POSTGRESQL) {
			resourceName = currentStructure == CDM ? cdm.constraintsPostgreSQL() : cdm.resultsConstraintsPostgreSQL();
		}
		
		if (resourceName != null) {
			boolean localDefinition = false;
			if (sourceFolder != null) {
				File localFile = new File(sourceFolder + "/Scripts/" + resourceName);
				if (localFile.exists()) {
					if (localFile.canRead()) {
						StringUtilities.outputWithTime("Using local definition: " + resourceName);
						resourceName = sourceFolder + "/Scripts/" + resourceName;
						localDefinition = true;
					}
					else {
						throw new RuntimeException("ERROR reading file: " + sourceFolder + "/Scripts/" + resourceName);
					}
				}
			}
			
			RichConnection connection = null;
			try {
				connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
				connection.setContext(cdm.getClass());
				
				StringUtilities.outputWithTime("Creating " + (currentStructure == CDM ? "CDM" : "Results")+ " constraints");
				connection.use(currentStructure == CDM ? dbSettings.database : dbSettings.resultsDatabase);
				if (localDefinition) {
					connection.executeLocalFile(resourceName);
				}
				else {
					resourceName = (version == VERSION_4 ? "" : (version == VERSION_501 ? "5.0.1/" : (version == VERSION_530 ? "5.3.0/" : (version == VERSION_531 ? "5.3.1/" : "6.0.0/")))) + resourceName;
					URL resourceURL = cdm.getClass().getResource(resourceName);
					if (resourceURL != null) {
						connection.executeResource(resourceName);
					}
					else {
						StringUtilities.outputWithTime("- " + (currentStructure == CDM ? "CDM" : "Results")+ " indices defintion not found.");
					}
				}
			} catch (Exception e) {
				handleError(e, frame, errorFolder, "Creating Constraints", continueOnError);
				if (!continueOnError && (JOptionPane.showConfirmDialog(frame, "Do you want to continue?","Continue?",JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION)) {
					throw new Exception("NO ERROR");
				}
			} finally {
				if (connection != null) {
					connection.close();
					StringUtilities.outputWithTime("Done");
				}
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
