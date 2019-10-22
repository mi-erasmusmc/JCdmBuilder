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

public interface CdmVx {
	public String structureMSSQL();

	public String structurePostgreSQL();

	public String structureOracle();

	public String constraintsMSSQL();

	public String constraintsPostgreSQL();

	public String constraintsOracle();

	public String indexesMSSQL();

	public String indexesPostgreSQL();

	public String indexesOracle();

	public String resultsMSSQL();

	public String resultsPostgreSQL();

	public String resultsOracle();

	public String resultsConstraintsMSSQL();

	public String resultsConstraintsPostgreSQL();

	public String resultsConstraintsOracle();

	public String resultsIndexesMSSQL();

	public String resultsIndexesPostgreSQL();

	public String resultsIndexesOracle();
}
