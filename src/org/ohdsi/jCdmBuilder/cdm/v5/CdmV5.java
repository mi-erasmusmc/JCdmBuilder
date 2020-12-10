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
package org.ohdsi.jCdmBuilder.cdm.v5;

import org.ohdsi.jCdmBuilder.cdm.CdmVx;

public class CdmV5 implements CdmVx {
	public String structureMSSQL() {
		//return "OMOP CDM Test sql server ddl.txt";
		return "OMOP CDM sql server ddl.txt";
	}

	public String structurePostgreSQL() {
		return "OMOP CDM postgresql ddl.txt";
	}

	public String structureOracle() {
		return "OMOP CDM oracle ddl.txt";
	}

	public String constraintsMSSQL() {
		//return "OMOP CDM Test sql server constraints.txt";
		return "OMOP CDM sql server constraints.txt";
	}

	public String constraintsPostgreSQL() {
		return "OMOP CDM postgresql constraints.txt";
	}

	public String constraintsOracle() {
		return "OMOP CDM oracle constraints.txt";
	}

	public String indexesMSSQL() {
		//return "OMOP CDM Test sql server pk indexes.txt";
		return "OMOP CDM sql server pk indexes.txt";
	}

	public String indexesPostgreSQL() {
		return "OMOP CDM postgresql pk indexes.txt";
	}

	public String indexesOracle() {
		return "OMOP CDM oracle pk indexes.txt";
	}

	@Override
	public String resultsStructureMSSQL() {
		return "OMOP CDM Results sql server ddl.txt";
	}

	@Override
	public String resultsStructurePostgreSQL() {
		return "OMOP CDM Results postgresql ddl.txt";
	}

	@Override
	public String resultsStructureOracle() {
		return "OMOP CDM Results oracle ddl.txt";
	}

	@Override
	public String resultsConstraintsMSSQL() {
		return null;
	}

	@Override
	public String resultsConstraintsPostgreSQL() {
		return null;
	}

	@Override
	public String resultsConstraintsOracle() {
		return null;
	}

	@Override
	public String resultsIndexesMSSQL() {
		return null;
	}

	@Override
	public String resultsIndexesPostgreSQL() {
		return null;
	}

	@Override
	public String resultsIndexesOracle() {
		return null;
	}
}
