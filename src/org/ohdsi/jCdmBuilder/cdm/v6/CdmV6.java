package org.ohdsi.jCdmBuilder.cdm.v6;

import org.ohdsi.jCdmBuilder.cdm.CdmVx;

public class CdmV6 implements CdmVx {

	@Override
	public String structureMSSQL() {
		return "OMOP CDM sql server ddl.sql";
	}

	@Override
	public String structurePostgreSQL() {
		return "OMOP CDM postgresql ddl.sql";
	}

	@Override
	public String structureOracle() {
		return "OMOP CDM oracle ddl.sql";
	}

	@Override
	public String indexesMSSQL() {
		return "OMOP CDM sql server pk indexes.sql";
	}

	@Override
	public String indexesPostgreSQL() {
		return "OMOP CDM postgresql pk indexes.sql";
	}

	@Override
	public String indexesOracle() {
		return "OMOP CDM oracle pk indexes.sql";
	}

	@Override
	public String resultsMSSQL() {
		return "OMOP CDM Results sql server ddl.sql";
	}

	@Override
	public String resultsPostgreSQL() {
		return "OMOP CDM Results postgresql ddl.sql";
	}

	@Override
	public String resultsOracle() {
		return "OMOP CDM Results oracle ddl.sql";
	}

	@Override
	public String resultsIndexesMSSQL() {
		return "OMOP CDM Results sql server pk indexes.sql";
	}

	@Override
	public String resultsIndexesPostgreSQL() {
		return "OMOP CDM Results postgresql pk indexes.sql";
	}

	@Override
	public String resultsIndexesOracle() {
		return "OMOP CDM Results oracle pk indexes.sql";
	}

}
