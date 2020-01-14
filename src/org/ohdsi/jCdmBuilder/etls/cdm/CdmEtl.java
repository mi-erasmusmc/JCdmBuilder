package org.ohdsi.jCdmBuilder.etls.cdm;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.swing.JFrame;
import javax.swing.JOptionPane;

import org.apache.commons.io.FileUtils;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.DbSettings;
import org.ohdsi.jCdmBuilder.ErrorReport;
import org.ohdsi.jCdmBuilder.JCdmBuilderMain;
import org.ohdsi.jCdmBuilder.cdm.Cdm;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.Row;

public class CdmEtl {
	
	public void process(int currentStructure, String folder, DbSettings dbSettings, int maxPersons, int versionId, String targetCmdVersion, JFrame frame, String errorFolder, boolean continueOnError) throws Exception {
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.use(currentStructure == Cdm.CDM ? dbSettings.database : dbSettings.resultsDatabase);
		
		Set<String> tables = new HashSet<String>();
		for (String table : connection.getTableNames(currentStructure == Cdm.CDM ? dbSettings.database : dbSettings.resultsDatabase))
			tables.add(table.toLowerCase());
		
		if (targetCmdVersion.equals("5.0.1")) {
			connection.execute("TRUNCATE TABLE _version");
			String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			connection.execute("INSERT INTO _version (version_id, version_date) VALUES (" + versionId + ", '" + date + "')");
		}

		for (File file : new File(folder).listFiles()) {
			String table = "";
			try {
				//System.out.println("  " + file.getAbsolutePath());
				if (file.getName().toLowerCase().endsWith(".csv")) {
					table = file.getName().substring(0, file.getName().length() - 4);
					if (tables.contains(table.toLowerCase())) {
						StringUtilities.outputWithTime("Inserting data for table " + table);
						connection.execute("TRUNCATE " + table);
						Iterator<Row> iterator = new ReadCSVFileWithHeader(file.getAbsolutePath()).iterator();
						Iterator<Row> filteredIterator = new RowFilterIterator(iterator, connection.getFieldNames(table), table);
						connection.insertIntoTable(filteredIterator, table, false, true);
					}
				}
			} catch (Exception e) {
				handleError(e, frame, errorFolder, "Table " + table, continueOnError);
				if (continueOnError || JOptionPane.showConfirmDialog(frame, "Do you want to continue with the next table?","Continue?",JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
					connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
					connection.use(currentStructure == Cdm.CDM ? dbSettings.database : dbSettings.resultsDatabase);
				}
				else {
					throw new Exception("NO ERROR");
				}
			}
		}
		StringUtilities.outputWithTime("Finished inserting tables");
	}
	
	public void process(int currentStructure, String folder, String temporaryServerFolder, String temporaryLocalServerFolder, DbSettings dbSettings, int maxPersons, int versionId, String targetCmdVersion, JFrame frame, String errorFolder, boolean continueOnError) throws Exception {
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.use(currentStructure == Cdm.CDM ? dbSettings.database : dbSettings.resultsDatabase);
		
		Set<String> tables = new HashSet<String>();
		for (String table : connection.getTableNames(currentStructure == Cdm.CDM ? dbSettings.database : dbSettings.resultsDatabase))
			tables.add(table.toLowerCase());
		
		if (targetCmdVersion.equals("5.0.1")) {
			connection.execute("TRUNCATE TABLE _version");
			String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			connection.execute("INSERT INTO _version (version_id, version_date) VALUES (" + versionId + ", '" + date + "')");
		}

		for (File file : new File(folder).listFiles()) {
			String table = "";
			try {
				if (file.getName().toLowerCase().endsWith(".csv")) {
					table = file.getName().substring(0, file.getName().length() - 4);
					if (tables.contains(table.toLowerCase())) {
						StringUtilities.outputWithTime("Inserting data for table " + table);
						connection.execute("TRUNCATE " + (currentStructure == Cdm.CDM ? dbSettings.database : dbSettings.resultsDatabase) + "." + table);

						String temporarySourceFileName = file.getName();
						String temporarySourceFileNamePath = file.getAbsolutePath();
						File temporarySourceFile = null;
						if (!folder.equals(temporaryLocalServerFolder)) {
							// Copy source file to temporary file on the server
							
							//PostgerSQL only
							String databaseName = dbSettings.server.split("/")[1].trim();
							
							temporarySourceFileName = databaseName + "_" + file.getName();
							StringUtilities.outputWithTime("Copy file " + file.getName() + " to " + temporarySourceFileName);
							temporarySourceFileNamePath = temporaryServerFolder + "/" + temporarySourceFileName;
							temporarySourceFile = new File(temporarySourceFileNamePath);
							FileUtils.copyFile(file, temporarySourceFile);
						}
						
						// Copy data into table
						
						// Postgresql
						StringUtilities.outputWithTime("Import file " + temporaryLocalServerFolder + "/" + temporarySourceFileName + " into table " + table);
						connection.execute("COPY " + (currentStructure == Cdm.CDM ? dbSettings.database : dbSettings.resultsDatabase) + "." + "." + table + " FROM '" + temporaryLocalServerFolder + "/" + temporarySourceFileName + "' WITH DELIMITER '" + dbSettings.delimiter + "' ENCODING 'WIN1252' CSV HEADER QUOTE '\"';");
						
						// SQL Server
						//connection.execute("BULK INSERT " + (currentStructure == Cdm.CDM ? dbSettings.database : dbSettings.resultsDatabase) + "." + table + " FROM '" + temporarySourceFileName + "' WITH (FORMAT = 'CSV', FIELDTERMINATOR = '" + dbSettings.delimiter + "', FIELDQUOTE = '\"', ROWTERMINATOR = '\n');");

						if (temporarySourceFile != null) {
							FileUtils.forceDelete(temporarySourceFile);
						}
					}
				}
			} catch (Exception e) {
				handleError(e, frame, errorFolder, "Table " + table, continueOnError);
				if (continueOnError || JOptionPane.showConfirmDialog(frame, "Do you want to continue with the next table?","Continue?",JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
					connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
					connection.use(currentStructure == Cdm.CDM ? dbSettings.database : dbSettings.resultsDatabase);
				}
				else {
					throw new Exception("NO ERROR");
				}
			}
		}
		StringUtilities.outputWithTime("Finished inserting tables");
	}
	
	private void handleError(Exception e, JFrame frame, String errorFolder, String item, boolean continueOnError) {
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
					filteredRow.add(normFieldName, row.get(fieldName));
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
