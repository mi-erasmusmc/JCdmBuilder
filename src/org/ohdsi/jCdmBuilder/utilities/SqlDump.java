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
package org.ohdsi.jCdmBuilder.utilities;

import org.ohdsi.jCdmBuilder.DbSettings;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteCSVFileWithHeader;

import org.ohdsi.databases.RichConnection;

public class SqlDump {
	public void process(DbSettings dbSettings, String sql, String filename) {
		StringUtilities.outputWithTime("Writing query results to file: " + filename);
		RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		connection.use(dbSettings.cdmSchema);
		WriteCSVFileWithHeader out = new WriteCSVFileWithHeader(filename);
		int rowCount = 0;
		for (Row row : connection.query(sql)) {
			out.write(row);
			rowCount++;
			if (rowCount % 100000 == 0)
				System.out.println(rowCount + " rows written to file");
		}
		System.out.println(rowCount + " rows written to file");
		out.close();
	}
}
