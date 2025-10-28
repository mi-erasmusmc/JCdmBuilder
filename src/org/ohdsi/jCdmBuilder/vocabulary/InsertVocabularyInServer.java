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
package org.ohdsi.jCdmBuilder.vocabulary;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.JFrame;
import javax.swing.JOptionPane;

import org.apache.commons.io.FileUtils;
import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.DbSettings;
import org.ohdsi.jCdmBuilder.ErrorReport;
import org.ohdsi.jCdmBuilder.JCdmBuilder;
import org.ohdsi.jCdmBuilder.utilities.ReadAthenaFile;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.FileUtilities;
import org.ohdsi.utilities.files.Row;

public class InsertVocabularyInServer {

	// private static String[] tables = new String[] { "CONCEPT", "CONCEPT_ANCESTOR", "CONCEPT_RELATIONSHIP", "CONCEPT_SYNONYM", "RELATIONSHIP",
	// "SOURCE_TO_CONCEPT_MAP", "VOCABULARY", "DRUG_STRENGTH", "DRUG_APPROVAL" };

	public void process(String folder, DbSettings dbSettings) {
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.use(dbSettings.cdmSchema);

		Set<String> tables = new HashSet<String>();
		for (String table : connection.getTableNames(dbSettings.cdmSchema))
			tables.add(table.toLowerCase());

		for (File file : new File(folder).listFiles()) {
			if (file.getName().toLowerCase().endsWith(".csv")) {
				String table = file.getName().substring(0, file.getName().length() - 4);
				if (tables.contains(table.toLowerCase())) {
					StringUtilities.outputWithTime("Inserting data for table " + table);
					connection.execute("TRUNCATE TABLE " + dbSettings.cdmSchema + "." + table);
					Iterator<Row> iterator = new ReadAthenaFile(file.getAbsolutePath()).iterator();
					Iterator<Row> filteredIterator = new RowFilterIterator(iterator, connection.getFieldNames(dbSettings.cdmSchema, table), table);
					Map<String, String> columnTypes = connection.getFieldTypes(dbSettings.cdmSchema, table);
					connection.insertIntoTable(filteredIterator, dbSettings.cdmSchema + "." + table, columnTypes, false, "");
				}
			}
		}
		StringUtilities.outputWithTime("Finished inserting tables");
	}
	
	public void process(String folder, String workingFolder, String temporaryServerFolder, String temporaryLocalServerFolder, DbSettings dbSettings, JFrame frame, String errorFolder) throws Exception {
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.use(dbSettings.cdmSchema);
		
		Set<String> tables = new HashSet<String>();
		for (String table : connection.getTableNames(dbSettings.cdmSchema))
			tables.add(table.toLowerCase());

		for (File file : new File(folder).listFiles()) {
			String table = "";
			List<String> fileParts = new ArrayList<String>();
			try {
				if (file.getName().toLowerCase().endsWith(".csv")) {
					table = file.getName().substring(0, file.getName().length() - 4);
					if (tables.contains(table.toLowerCase())) {
						StringUtilities.outputWithTime("Inserting data for table " + table);
						connection.execute("TRUNCATE TABLE " + dbSettings.cdmSchema + "." + table);

						String temporarySourceFileNamePath = file.getAbsolutePath();
						if (!folder.equals(temporaryLocalServerFolder)) {
							// Copy source file to temporary file on the server

							String databaseName = "";
							// PostgreSQL
							if (dbSettings.dbType == DbType.POSTGRESQL) {
								databaseName = dbSettings.server.split("/")[1].trim();
							}
							// Microsoft SQL Server
							else if (dbSettings.dbType == DbType.MSSQL) {
								databaseName = dbSettings.server.substring(dbSettings.server.indexOf("database=") + 9).trim();
								if (databaseName.contains(";")) {
									databaseName = databaseName.substring(0, databaseName.indexOf(";")).trim();
								}
							}
							/* Split always to automatically convert character sets
							if (FileUtils.sizeOf(file) < 2000000000L) {
								temporarySourceFileName = databaseName + "_" + dbSettings.cdmSchema + "_" + file.getName();
								temporarySourceFileNamePath = temporaryServerFolder + File.separator + temporarySourceFileName;
								File temporarySourceFile = new File(temporarySourceFileNamePath);
								StringUtilities.outputWithTime("Copy file " + file.getName() + " to " + temporarySourceFile.getAbsolutePath());
								fileParts.add(temporarySourceFileName);
								FileUtils.copyFile(file, temporarySourceFile);
								//fileParts = FileUtilities.copyCSVFile(file, workingFolder, temporaryServerFolder, databaseName + "_" + dbSettings.cdmSchema, (char) 0);
							}
							else { // Split file in parts of less than 2 GB.
								fileParts = FileUtilities.splitCSVFile(file, workingFolder, temporaryServerFolder, databaseName + "_" + dbSettings.cdmSchema, (char) 0, 2000000000);
							}
							*/
							//fileParts = FileUtilities.oldSplitCSVFile(file, workingFolder, temporaryServerFolder, databaseName + "_" + dbSettings.cdmSchema, (char) 0, 2000000000);
							Map<String, Integer> varCharColumnLengths = connection.getVarcharColumnLengths(connection.getFieldTypes(dbSettings.cdmSchema, table));
							fileParts = FileUtilities.splitCSVFile(file, varCharColumnLengths, workingFolder, temporaryServerFolder, databaseName + "_" + dbSettings.cdmSchema, '\t', (char) 0, 2000000000);
						}
						
						// Copy data into table
						for (String fileName : fileParts) {
							// PostgreSQL
							if (dbSettings.dbType == DbType.POSTGRESQL) {
								StringUtilities.outputWithTime("Import file " + temporaryLocalServerFolder + File.separator + fileName + " into table " + table);
								// Use delete character as quote
								connection.execute("COPY " + dbSettings.cdmSchema + "." + table + " FROM '" + temporaryLocalServerFolder + File.separator + fileName + "' WITH DELIMITER '\t' NULL '' ENCODING 'WIN1252' CSV HEADER QUOTE '" + ((char) 127) + "';");
							}
							// Microsoft SQL Server
							else if (dbSettings.dbType == DbType.MSSQL) {
								connection.execute("BULK INSERT " + dbSettings.cdmSchema + "." + table + " FROM '" + temporaryLocalServerFolder + File.separator + fileName + "' WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = '\t', FIELDQUOTE = '', ROWTERMINATOR = '\n');");
							}
							// Oracle
							else if (dbSettings.dbType == DbType.ORACLE) {
								connection.execute("LOAD DATA INFILE '" + temporaryLocalServerFolder + File.separator + fileName + "' INTO TABLE " + dbSettings.cdmSchema + "." + table + " FIELD TERMINATED BY '\t' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '\n';");
							}

							temporarySourceFileNamePath = temporaryServerFolder + File.separator + fileName;
							File temporarySourceFile = new File(temporarySourceFileNamePath);
							if (temporarySourceFile != null) {
								FileUtils.forceDelete(temporarySourceFile);
							}
						}
					}
				}
			} catch (Exception e) {
				// Delete files from the server
				for (String fileName : fileParts) {
					File temporarySourceFile = new File(temporaryServerFolder + File.separator + fileName);
					if (temporarySourceFile != null) {
						FileUtils.forceDelete(temporarySourceFile);
					}
				}
				
				handleError(e, frame, errorFolder, "Table " + table, false);
				throw new Exception(e);
			}
		}
		
		StringUtilities.outputWithTime("Finished inserting tables");
	}
	
	private void handleError(Exception e, JFrame frame, String errorFolder, String item, boolean continueOnError) {
		JCdmBuilder.errors.add(item);
		System.err.println("Error: " + e.getMessage());
		String errorReportFilename = ErrorReport.generate(errorFolder, e, item);
		String message = "Error: " + e.getLocalizedMessage();
		message += "\nAn error report has been generated:\n" + errorReportFilename;
		System.out.println(message);
		if (!continueOnError) {
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error", JOptionPane.ERROR_MESSAGE);
		}
	}

	private static class RowFilterIterator implements Iterator<Row> {

		private Set<String>		allowedFields;
		private Iterator<Row>	iterator;
		private Set<String>		ignoredFields;

		public RowFilterIterator(Iterator<Row> iterator, Collection<String> allowedFields, String tableName) {
			this.allowedFields = new HashSet<String>();
			for (String field : allowedFields)
				this.allowedFields.add(field.toLowerCase());
			this.iterator = iterator;
			ignoredFields = new HashSet<String>();
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public Row next() {
			Row row = iterator.next();
			Row filteredRow = new Row();
			for (String fieldName : row.getFieldNames()) {
				if (allowedFields.contains(fieldName.toLowerCase()))
					filteredRow.add(fieldName, row.get(fieldName, true));
				else if (ignoredFields.add(fieldName))
					System.err.println("Ignoring field " + fieldName);
			}
			return filteredRow;
		}

		@Override
		public void remove() {
			throw new RuntimeException("Calling unimplemented method");
		}
	}
}
