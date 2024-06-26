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

	public String indicesMSSQL();

	public String indicesPostgreSQL();

	public String indicesOracle();

	public String primaryKeysMSSQL();

	public String primaryKeysPostgreSQL();

	public String primaryKeysOracle();

	public String resultsStructureMSSQL();

	public String resultsStructurePostgreSQL();

	public String resultsStructureOracle();
}
