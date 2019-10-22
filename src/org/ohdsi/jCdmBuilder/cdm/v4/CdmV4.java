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
package org.ohdsi.jCdmBuilder.cdm.v4;

import org.ohdsi.jCdmBuilder.cdm.CdmVx;

public class CdmV4 implements CdmVx {
	public String structureMSSQL() {
		return "CreateCDMStructureSQLServer.sql";
	}

	public String structurePostgreSQL() {
		return "CreateCDMStructurePostgreSQL.sql";
	}

	public String structureOracle() {
		return "CreateCDMStructureOracle.sql";
	}

	public String constraintsMSSQL() {
		return null;
	}

	public String constraintsPostgreSQL() {
		return null;
	}

	public String constraintsOracle() {
		return null;
	}

	public String indexesMSSQL() {
		return "CreateCDMIndicesSQLServer.sql";
	}

	public String indexesPostgreSQL() {
		return "CreateCDMIndices.sql";
	}

	public String indexesOracle() {
		return "CreateCDMIndices.sql";
	}

	@Override
	public String resultsMSSQL() {
		return null;
	}

	@Override
	public String resultsPostgreSQL() {
		return null;
	}

	@Override
	public String resultsOracle() {
		return null;
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
