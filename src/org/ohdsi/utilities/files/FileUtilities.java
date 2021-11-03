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
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.ohdsi.utilities.StringUtilities;

public class FileUtilities {
	static int CR = 13;
	static int LF = 10;
	
	public static void decompressGZIP(String sourceFilename, String targetFilename) throws FileNotFoundException, IOException{
		copyStream(new GZIPInputStream(new FileInputStream(sourceFilename)), new FileOutputStream(targetFilename)); 
	}
	
	public static List<String> splitCSVFile(File file, String tempFolder, String destinationFolder, String fileNamePrefix, char quote, int maxSize) throws IOException {
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
}
