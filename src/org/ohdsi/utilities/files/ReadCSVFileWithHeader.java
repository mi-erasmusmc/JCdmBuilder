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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ReadCSVFileWithHeader implements Iterable<Row> {
	private InputStream	inputstream;
	private char delimiter = ',';
	private char quote = '"';
	private List<String> header = null;

	public ReadCSVFileWithHeader(String filename, char delimiter, char quote) {
		this(filename);
		this.delimiter = delimiter;
		this.quote = quote;
	}

	public ReadCSVFileWithHeader(String filename) {
		try {
			inputstream = new FileInputStream(filename);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public ReadCSVFileWithHeader(InputStream inputstream) {
		this.inputstream = inputstream;
	}
	
	public List<String> getHeader() {
		 return header;
	}

	@Override
	public Iterator<Row> iterator() {
		return new RowIterator();
	}

	public class RowIterator implements Iterator<Row> {

		private Iterator<List<String>>	iterator;
		private Map<String, Integer>	fieldName2ColumnIndex;

		public RowIterator() {
			iterator = new ReadCSVFile(inputstream, delimiter, quote).iterator();
			fieldName2ColumnIndex = new HashMap<String, Integer>();
			header = iterator.next();
			for (String columnHeader : header)
				fieldName2ColumnIndex.put(columnHeader, fieldName2ColumnIndex.size());
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public Row next() {
			return new Row(iterator.next(), fieldName2ColumnIndex);
		}

		@Override
		public void remove() {
			throw new RuntimeException("Remove not supported");
		}

	}
}
