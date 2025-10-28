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
package org.ohdsi.utilities.files;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.ohdsi.utilities.StringUtilities;

public class FileUtilities {
	static int CR = 13;
	static int LF = 10;
	static String EOL = "\r\n"; //File.separator.equals("/") ? "\n" : "\r\n";
	
	public static void decompressGZIP(String sourceFilename, String targetFilename) throws FileNotFoundException, IOException{
		copyStream(new GZIPInputStream(new FileInputStream(sourceFilename)), new FileOutputStream(targetFilename)); 
	}
	
	
	public static List<String>  copyCSVFile(File file, String tempFolder, String destinationFolder, String fileNamePrefix, char quote) throws IOException {
		List<String> fileParts = new ArrayList<String>();
		String partFileName = fileNamePrefix + "_" + file.getName();
		String temporaryFileNamePath = tempFolder + File.separator + partFileName;
		String destinationFileNamePath = destinationFolder + File.separator + partFileName;
		StringUtilities.outputWithTime("    " + temporaryFileNamePath);
		StringUtilities.outputWithTime("Copy file " + file.getName() + " to " + destinationFileNamePath);
		BufferedReader fileReader = new BufferedReader(new FileReader(file));
		BufferedWriter fileWriter = null;
		String record = getNextCSVRecord(fileReader, (int) quote);
		String header = null;
		while (record != null) {
			if (fileWriter == null) {
				fileWriter = new BufferedWriter(new FileWriter(new File(temporaryFileNamePath)));
				if (header == null) {
					header = record;
				}
				else {
					fileWriter.append(header);
				}
			}
			fileWriter.append(record);
			record = getNextCSVRecord(fileReader, (int) quote);
		}
		if (fileWriter != null) {
			fileWriter.close();
			if (!temporaryFileNamePath.equals(destinationFileNamePath)) {
				File tempFile = new File(temporaryFileNamePath);
				File destinationFile = new File(destinationFileNamePath);
				StringUtilities.outputWithTime("    Copy " + temporaryFileNamePath + " to " + destinationFileNamePath);
				FileUtils.copyFile(tempFile, destinationFile);
				StringUtilities.outputWithTime("    Delete " + temporaryFileNamePath);
				FileUtils.forceDelete(tempFile);
			}
		}
		StringUtilities.outputWithTime("Copy file " + file.getName() + " to " + destinationFileNamePath + " finished");
		fileParts.add(partFileName);
		return fileParts;
	}
	
	public static List<String> oldSplitCSVFile(File file, Map<String, String> columnTypes, String tempFolder, String destinationFolder, String fileNamePrefix, char delimiter, char quote, int maxSize) throws IOException {
		StringUtilities.outputWithTime("Split file " + file.getName() + " into:");
		List<String> fileParts = new ArrayList<String>();
		int fileNr = 0;
		int fileSize = 0;
		BufferedReader fileReader = new BufferedReader(new FileReader(file));
		BufferedWriter fileWriter = null;
		String record = getNextCSVRecord(fileReader, (int) quote);
		String header = null;
		String temporaryPartFileNamePath = null;
		String destinationPartFileNamePath = null;
		while (record != null) {
			if (fileWriter == null) {
				fileNr++;
				String partFileName = fileNamePrefix + "_" + Integer.toString(fileNr) + "_" + file.getName();
				temporaryPartFileNamePath = tempFolder + File.separator + partFileName;
				destinationPartFileNamePath = destinationFolder + File.separator + partFileName;
				fileParts.add(partFileName);
				StringUtilities.outputWithTime("    " + temporaryPartFileNamePath);
				fileWriter = new BufferedWriter(new FileWriter(new File(temporaryPartFileNamePath)));
				if (header == null) {
					header = record;
				}
				else {
					fileWriter.append(header);
					fileSize += header.length();
				}
			}
			fileWriter.append(record);
			fileSize += record.length();
			if (fileSize > maxSize) {
				fileWriter.close();
				if (!temporaryPartFileNamePath.equals(destinationPartFileNamePath)) {
					File tempFile = new File(temporaryPartFileNamePath);
					File destinationFile = new File(destinationPartFileNamePath);
					StringUtilities.outputWithTime("    Copy " + temporaryPartFileNamePath + " to " + destinationPartFileNamePath);
					FileUtils.copyFile(tempFile, destinationFile);
					StringUtilities.outputWithTime("    Delete " + temporaryPartFileNamePath);
					FileUtils.forceDelete(tempFile);
				}
				fileWriter = null;
				fileSize = 0;
			}
			record = getNextCSVRecord(fileReader, (int) quote);
		}
		if (fileWriter != null) {
			fileWriter.close();
			if (!temporaryPartFileNamePath.equals(destinationPartFileNamePath)) {
				File tempFile = new File(temporaryPartFileNamePath);
				File destinationFile = new File(destinationPartFileNamePath);
				StringUtilities.outputWithTime("    Copy " + temporaryPartFileNamePath + " to " + destinationPartFileNamePath);
				FileUtils.copyFile(tempFile, destinationFile);
				StringUtilities.outputWithTime("    Delete " + temporaryPartFileNamePath);
				FileUtils.forceDelete(tempFile);
			}
		}
		StringUtilities.outputWithTime("Split file " + file.getName() + " finished");
		return fileParts;
	}
	
	public static List<String> splitCSVFile(File file, Map<String, Integer> varcharColumnLengths, String tempFolder, String destinationFolder, String fileNamePrefix, char delimiter, char quote, int maxSize) throws IOException {
		StringUtilities.outputWithTime("Split file " + file.getName() + " into:");
		List<String> fileParts = new ArrayList<String>();
		int fileNr = 0;
		int fileSize = 0;
		String header = "";
		String temporaryPartFileNamePath = null;
		String destinationPartFileNamePath = null;
		
		ReadCSVFileWithHeader csvReader = new ReadCSVFileWithHeader(file.getAbsolutePath(), delimiter, quote);
		Iterator<Row> csvReaderIterator = csvReader.iterator();
		BufferedWriter fileWriter = null;
		
		// Convert uppercase column names in varcharColumnLengths to the header column names in the file.
		for (String headerColumn : csvReader.getHeader()) {
			String uppercaseHeaderColumn = headerColumn.toUpperCase();
			if (!headerColumn.equals(uppercaseHeaderColumn)) {
				varcharColumnLengths.put(headerColumn, varcharColumnLengths.get(uppercaseHeaderColumn));
				varcharColumnLengths.remove(uppercaseHeaderColumn);
			}
			header += (header.equals("") ? "" : delimiter) + headerColumn; 
		}
		
		while (csvReaderIterator.hasNext()) {
			if (fileWriter == null) {
				fileNr++;
				String partFileName = fileNamePrefix + "_" + Integer.toString(fileNr) + "_" + file.getName();
				temporaryPartFileNamePath = tempFolder + File.separator + partFileName;
				destinationPartFileNamePath = destinationFolder + File.separator + partFileName;
				fileParts.add(partFileName);
				StringUtilities.outputWithTime("    " + temporaryPartFileNamePath);
				fileWriter = new BufferedWriter(new FileWriter(new File(temporaryPartFileNamePath)));
				fileWriter.append(header + EOL);
				fileSize += header.length() + EOL.length();
			}
			Row row = csvReaderIterator.next();
			
			if ((varcharColumnLengths != null) && (varcharColumnLengths.keySet().size() > 0)) {
				for (String column : varcharColumnLengths.keySet()) {
					row.set(column, StringUtilities.truncateStringIfTooLong(row.get(column, false), varcharColumnLengths.get(column)));
				}
			}
			String record = row.toCSVString(delimiter, quote) + EOL;
			fileWriter.append(record);
			fileSize += record.length();
			if (fileSize > maxSize) {
				fileWriter.close();
				if (!temporaryPartFileNamePath.equals(destinationPartFileNamePath)) {
					File tempFile = new File(temporaryPartFileNamePath);
					File destinationFile = new File(destinationPartFileNamePath);
					if (!temporaryPartFileNamePath.equals(destinationPartFileNamePath)) {
						StringUtilities.outputWithTime("    Copy " + temporaryPartFileNamePath + " to " + destinationPartFileNamePath);
						FileUtils.copyFile(tempFile, destinationFile);
					}
					StringUtilities.outputWithTime("    Delete " + temporaryPartFileNamePath);
					FileUtils.forceDelete(tempFile);
				}
				fileWriter = null;
				fileSize = 0;
			}
		}
		if (fileWriter != null) {
			fileWriter.close();
			if (!temporaryPartFileNamePath.equals(destinationPartFileNamePath)) {
				File tempFile = new File(temporaryPartFileNamePath);
				File destinationFile = new File(destinationPartFileNamePath);
				if (!temporaryPartFileNamePath.equals(destinationPartFileNamePath)) {
					StringUtilities.outputWithTime("    Copy " + temporaryPartFileNamePath + " to " + destinationPartFileNamePath);
					FileUtils.copyFile(tempFile, destinationFile);
				}
				StringUtilities.outputWithTime("    Delete " + temporaryPartFileNamePath);
				FileUtils.forceDelete(tempFile);
			}
		}

		StringUtilities.outputWithTime("Split file " + file.getName() + " finished");
		return fileParts;
	}
	
	private static final void copyStream(InputStream source, OutputStream dest){
		int bufferSize = 1024;
		int bytes;
		byte[] buffer;
		buffer = new byte[bufferSize];
		try {  
			while ((bytes = source.read(buffer)) != -1) {
				if (bytes == 0) {
					bytes = source.read();
					if (bytes < 0)
						break;
					dest.write(bytes);
					dest.flush();
					continue;
				}
				dest.write(buffer, 0, bytes);
				dest.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}  
	}
	
	private static String getNextCSVRecord(BufferedReader csvFileReader, int quote) throws IOException {
		String record = null;
		boolean eol = false;
		boolean quoted = false;
		int lastCharNum = -2;
		while (!eol) {
			int charNum = lastCharNum == -2 ? csvFileReader.read() : lastCharNum;
			lastCharNum = -2;
			if (charNum != -1) {
				if (!quoted) {
					if (charNum == LF) {
						record = (record == null ? "" : record) + ((char)charNum);
						eol = true;
						break;
					}
					else if ((quote != 0) && (charNum == quote)) {
						quoted = true;
						record = (record == null ? "" : record) + ((char)charNum);
					}
					else {
						record = (record == null ? "" : record) + ((char)charNum);
					}
				}
				else {
					if ((quote != 0) && (charNum == quote)) {
						record = (record == null ? "" : record) + ((char)charNum);
						charNum = csvFileReader.read();
						if (charNum != quote) {
							quoted = false;
							lastCharNum = charNum;
						}
						else {
							record = (record == null ? "" : record) + ((char)charNum);
						}
					}
					else {
						record = (record == null ? "" : record) + ((char)charNum);
					}
				}
			}
			else {
				// EOF
				break;
			}
		}
		return record;
	}


	public static void main(String[] args) {
		Map<String, Integer> varcharColumnLengths = new HashMap<String, Integer>();
		varcharColumnLengths.put("COLUMN1", 20);
		varcharColumnLengths.put("COLUMN2", 25);
		varcharColumnLengths.put("COLUMN3", 30);
		try {
			FileUtilities.splitCSVFile(new File("D:\\Temp\\CDM\\Temp\\TestFile.csv"), varcharColumnLengths, "D:\\Temp\\CDM\\Temp", "D:\\Temp", "Split", ',', '"', 1000000);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
