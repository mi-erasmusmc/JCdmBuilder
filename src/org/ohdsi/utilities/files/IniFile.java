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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IniFile {
	private String fileName;
	private File iniFile;
	private String error = "";
	private Map<String, Map<String, String>> iniFileMap = new HashMap<String, Map<String, String>>();
	private Map<String, List<String>> groupComments = new HashMap<String, List<String>>();
	private Map<String, Map<String, List<String>>> variableComments = new HashMap<String, Map<String, List<String>>>();
	private List<String> groupOrder = new ArrayList<String>();
	private Map<String, List<String>> groupVariableOrder = new HashMap<String, List<String>>();
	
	
	public IniFile(String iniFileName) {
		setFileName(iniFileName);
	}
	
	
	public void setFileName(String iniFileName) {
		fileName = iniFileName;
		iniFile = new File(fileName);
	}
	
	
	public String getFileName() {
		return fileName;
	}
	
	
	public boolean readFile() {
		boolean ok = false;
		if (iniFile.canRead()) {
			try {
				BufferedReader iniFileReader = new BufferedReader(new FileReader(iniFile));
				String currentGroupName = null;
				List<String> comments = null;
				String line = iniFileReader.readLine();
				while (line != null) {
					line = line.trim();
					if (!line.equals("")) {
						if (line.startsWith("#")) {
							if (comments == null) {
								comments = new ArrayList<String>();
							}
							comments.add(line);
						}
						else if (line.startsWith("[") && line.endsWith("]")) {
							currentGroupName = line.substring(1, line.length() - 1).trim();
							addGroup(currentGroupName, comments);
							comments = null;
						}
						else {
							if (currentGroupName != null) {
								String variable = "";
								String value = null;
								if (line.contains("=")) {
									int assignmentIndex = line.indexOf("=");
									variable = line.substring(0, assignmentIndex).trim();
									value = line.substring(assignmentIndex + 1).trim(); 
								}
								else {
									variable = line;
								}
								setValue(currentGroupName, variable, value, comments);
								comments = null;
							}
						}
					}
					line = iniFileReader.readLine();
				}
				iniFileReader.close();
				ok = true;
			} catch (FileNotFoundException e) {
				error = "Error while reading ini file '" + fileName + "'";
			} catch (IOException e) {
				error = "Error while reading ini file '" + fileName + "'";
			}
		}
		else {
			error = "Cannot read ini file '" + fileName + "'";
		}
		
		return ok;
	}
	
	
	public boolean writeFile() {
		boolean ok = false;
		PrintStream stream;
		try {
			stream = new PrintStream(iniFile);
			writeFile(stream);
			stream.close();
			ok = true;
		} catch (FileNotFoundException e) {
			error = "Cannot write ini file '" + fileName + "'";
		}
		return ok;
	}
	
	
	public boolean writeFile(PrintStream stream) {
		boolean ok = false;

		boolean firstGroup = true;
		for (String groupName : groupOrder) {
			if (!firstGroup) {
				stream.println();
				stream.println();
			}
			if (groupComments.get(groupName) != null) {
				for (String comment : groupComments.get(groupName)) {
					stream.println(comment);
				}
			}
			stream.println("[" + groupName + "]");
			
			if (groupVariableOrder.get(groupName) != null) {
				for (String variable : groupVariableOrder.get(groupName)) {
					if ((variableComments.get(groupName) != null) && (variableComments.get(groupName).get(variable) != null)) {
						for (String comment : variableComments.get(groupName).get(variable)) {
							stream.println(comment);
						}
					}
					String value = getValue(groupName, variable);
					if (value != null) {
						stream.println(variable + " = " + value);
					}
					else {
						stream.println(variable);
					}
				}
			}
			
			firstGroup = false;
		}
		
		return ok;
	}
	
	
	public String getError() {
		return error;
	}
	
	
	public boolean hasGroup(String group) {
		return iniFileMap.keySet().contains(group);
	}
	
	
	public boolean addGroup(String group, List<String> comments) {
		if (!hasGroup(group)) {
			iniFileMap.put(group, new HashMap<String, String>());
			groupOrder.add(group);
			groupVariableOrder.put(group, new ArrayList<String>());
		}
		return setGroupComments(group, comments);
	}
	
	
	public boolean setGroupComments(String group, List<String> comments) {
		boolean result = false;
		if (hasGroup(group)) {
			groupComments.put(group, comments);
			result = true;
		}
		return result;
	}
	
	
	public List<String> getGroups() {
		return groupOrder;
	}
	
	
	public Map<String, String> getGroup(String group) {
		return iniFileMap.get(group);
	}
	
	
	public List<String> getGroupVariables(String group) {
		return groupVariableOrder.get(group);
	}
	
	
	public boolean hasVariable(String group, String variable) {
		return (hasGroup(group) && groupVariableOrder.get(group).contains(variable));
	}
	
	
	public boolean setVariableComments(String group, String variable, List<String> comments) {
		boolean result = false;
		if (hasVariable(group, variable)) {
			Map<String, List<String>> groupVariableComments = variableComments.get(group);
			if (groupVariableComments == null) {
				groupVariableComments = new HashMap<String, List<String>>();
				variableComments.put(group, groupVariableComments);
			}
			groupVariableComments.put(variable, comments);
			result = true;
		}
		return result;
	}
	
	
	public String getValue(String group, String variable) {
		String value = null;
		if (hasVariable(group, variable)) {
			value = iniFileMap.get(group).get(variable);
		}
		return value;
	}
	
	
	public boolean setValue(String group, String variable, String value, List<String> comments) {
		boolean result = false;
		if (hasGroup(group) || addGroup(group, null)) {
			if (!hasVariable(group, variable)) {
				List<String> variableOrder = groupVariableOrder.get(group);
				if (variableOrder == null) {
					variableOrder = new ArrayList<String>();
					groupVariableOrder.put(group, variableOrder);
				}
				variableOrder.add(variable);
			}
			Map<String, String> variableValues = iniFileMap.get(group);
			if (variableValues == null) {
				variableValues = new HashMap<String, String>();
				iniFileMap.put(group, variableValues);
			}
			variableValues.put(variable, value);
			result = setVariableComments(group, variable, comments);
		}
		return result;
	}
	
	
	public List<String> getListValue(String group, String variable, String listSeparator) {
		List<String> list = null;
		String listString = getValue(group, variable);
		if (listString != null) {
			String[] listStringSplit = listString.split(listSeparator);
			for (String listItem : listStringSplit) {
				listItem = listItem.trim();
				if (!listItem.equals("")) {
					if (list == null) {
						list = new ArrayList<String>();
					}
					list.add(listItem);
				}
			}
		}
		return list;
	}
	
	
	public boolean setListValue(String group, String variable, List<String> listValue, String listSeparator, List<String> comments) {
		boolean result = false;
		if (hasGroup(group) || addGroup(group, null)) {
			if (!hasVariable(group, variable)) {
				List<String> variableOrder = groupVariableOrder.get(group);
				if (variableOrder == null) {
					variableOrder = new ArrayList<String>();
					groupVariableOrder.put(group, variableOrder);
				}
				variableOrder.add(variable);
			}
			Map<String, String> variableValues = iniFileMap.get(group);
			if (variableValues == null) {
				variableValues = new HashMap<String, String>();
				iniFileMap.put(group, variableValues);
			}
			if ((listValue != null) && (listValue.size() > 0)) {
				String value = "";
				for (int index = 0; index < listValue.size(); index++) {
					value = value + (index > 0 ? listSeparator : "") + listValue.get(index);
				}
				variableValues.put(variable, value);
			}
			result = setVariableComments(group, variable, comments);
		}
		return result;
	}
	
	
	//public List<String> getVariableComments(String group, String variable) {
	//	return (hasGroup(group))
	//}

}
