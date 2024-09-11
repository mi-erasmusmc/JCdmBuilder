package org.ohdsi.jCdmBuilder.etls.cdm;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
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
import org.ohdsi.jCdmBuilder.cdm.Cdm;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.FileUtilities;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.Row;

public class CdmEtl {
	
	public void process(int currentStructure, String folder, String delimiterString, String quoteString, String nullValueString, DbSettings dbSettings, int maxPersons, int versionId, String targetCmdVersion, JFrame frame, String errorFolder, boolean continueOnError) throws Exception {
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.use(currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
		
		char delimiter = delimiterString.trim().toLowerCase().equals("tab") ? '\t' : delimiterString.charAt(0);
		char quote     = quoteString.charAt(0);
		
		Set<String> tables = new HashSet<String>();
		for (String table : connection.getTableNames(currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema))
			tables.add(table.toLowerCase());
		
		if (targetCmdVersion.equals("5.0.1")) {
			connection.execute("TRUNCATE TABLE " + (currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema) + "." + "_version");
			String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			connection.execute("INSERT INTO " + (currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema) + "." + "_version (version_id, version_date) VALUES (" + versionId + ", '" + date + "')");
		}

		for (File file : new File(folder).listFiles()) {
			String table = "";
			try {
				//System.out.println("  " + file.getAbsolutePath());
				if (file.getName().toLowerCase().endsWith(".csv")) {
					table = file.getName().substring(0, file.getName().length() - 4);
					if (tables.contains(table.toLowerCase())) {
						StringUtilities.outputWithTime("Inserting data for table " + table);
						connection.execute("TRUNCATE TABLE " + (currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema) + "." + table);
						Iterator<Row> iterator = new ReadCSVFileWithHeader(file.getAbsolutePath(), delimiter, quote).iterator();
						Iterator<Row> filteredIterator = new RowFilterIterator(iterator, connection.getFieldNames(dbSettings.cdmSchema, table), table);
						Map<String, String> columnTypes = connection.getFieldTypes(dbSettings.cdmSchema, table);
						connection.insertIntoTable(filteredIterator, (currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema) + "." + table, columnTypes, false, nullValueString);
					}
				}
			} catch (Exception e) {
				handleError(e, frame, errorFolder, "Table " + table, continueOnError);
				if (continueOnError || JOptionPane.showConfirmDialog(frame, "Do you want to continue with the next table?","Continue?",JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
					connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
					connection.use(currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
				}
				else {
					throw new Exception("NO ERROR");
				}
			}
		}
		StringUtilities.outputWithTime("Finished inserting tables");
	}
	
	public void process(int currentStructure, String folder, String delimiterString, String quoteString, String nullValueString, String workingFolder, String temporaryServerFolder, String temporaryLocalServerFolder, DbSettings dbSettings, int maxPersons, int versionId, String targetCdmVersion, JFrame frame, String errorFolder, boolean continueOnError) throws Exception {
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.use(currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
		
		char delimiter = delimiterString.trim().toLowerCase().equals("tab") ? '\t' : delimiterString.charAt(0);
		char quote     = quoteString.charAt(0);
		
		Set<String> tables = new HashSet<String>();
		for (String table : connection.getTableNames(currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema))
			tables.add(table.toLowerCase());
		
		if (targetCdmVersion.equals("5.0.1")) {
			connection.execute("TRUNCATE TABLE _version");
			String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			connection.execute("INSERT INTO _version (version_id, version_date) VALUES (" + versionId + ", '" + date + "')");
		}

		for (File file : new File(folder).listFiles()) {
			String table = "";
			List<String> fileParts = new ArrayList<String>();
			try {
				if (file.getName().toLowerCase().endsWith(".csv")) {
					table = file.getName().substring(0, file.getName().length() - 4);
					if (tables.contains(table.toLowerCase())) {
						StringUtilities.outputWithTime("Inserting data for table " + table);
						connection.execute("TRUNCATE TABLE " + (currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema) + "." + table);

						String temporarySourceFileName = file.getName();
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
							}
							else { // Split file in parts of less than 2 GB.
								fileParts = FileUtilities.splitCSVFile(file, workingFolder, temporaryServerFolder, databaseName + "_" + dbSettings.cdmSchema, quote, 2000000000);
							}
							*/
							Map<String, String> columnTypes = connection.getFieldTypes(dbSettings.cdmSchema, table);
							fileParts = FileUtilities.splitCSVFile(file, workingFolder, temporaryServerFolder, databaseName + "_" + dbSettings.cdmSchema, quote, 2000000000);
						}
						
						// Copy data into table
						for (String fileName : fileParts) {
							// PostgreSQL
							if (dbSettings.dbType == DbType.POSTGRESQL) {
								StringUtilities.outputWithTime("Import file " + temporaryLocalServerFolder + "/" + fileName + " into table " + table);
								connection.execute("COPY " + (currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema) + "." + table + " FROM '" + temporaryLocalServerFolder + "/" + fileName + "' WITH DELIMITER '" + delimiter + "' NULL '" + (nullValueString == null ? "" : nullValueString) + "' ENCODING 'WIN1252' CSV HEADER QUOTE '\"';");
							}
							// Microsoft SQL Server
							else if (dbSettings.dbType == DbType.MSSQL) {
								connection.execute("BULK INSERT " + (currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema) + "." + table + " FROM '" + temporaryLocalServerFolder + "/" + fileName + "' WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = '" + delimiter + "', FIELDQUOTE = '" + quote + "', ROWTERMINATOR = '\n');");
							}
							// Oracle
							else if (dbSettings.dbType == DbType.ORACLE) {
								connection.execute("LOAD DATA INFILE '" + temporaryLocalServerFolder + "/" + fileName + "' INTO TABLE " + (currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema) + "." + table + " FIELD TERMINATED BY '" + delimiter + "' OPTIONALLY ENCLOSED BY '" + quote + "' LINES TERMINATED BY '\n';");
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
				
				handleError(e, frame, errorFolder, "Table " + table, continueOnError);
				if (continueOnError || JOptionPane.showConfirmDialog(frame, "Do you want to continue with the next table?","Continue?",JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
					connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
					connection.use(currentStructure == Cdm.CDM ? dbSettings.cdmSchema : dbSettings.resultsSchema);
				}
				else {
					throw new Exception("NO ERROR");
				}
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
				String normFieldName = fieldName.toLowerCase().trim();
				if (allowedFields.contains(normFieldName))
					filteredRow.add(normFieldName, row.get(fieldName, true));
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
