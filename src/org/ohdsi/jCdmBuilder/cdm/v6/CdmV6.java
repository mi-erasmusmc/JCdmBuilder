package org.ohdsi.jCdmBuilder.cdm.v6;

import org.ohdsi.jCdmBuilder.cdm.CdmVx;

public class CdmV6 implements CdmVx {

	@Override
	public String structureMSSQL() {
		return "OMOP CDM sql server ddl.txt";
	}

	@Override
	public String structurePostgreSQL() {
		return "OMOP CDM postgresql ddl.txt";
	}

	@Override
	public String structureOracle() {
		return "OMOP CDM oracle ddl.txt";
	}

	@Override
	public String constraintsMSSQL() {
		return null;
	}

	@Override
	public String constraintsPostgreSQL() {
		return null;
	}

	@Override
	public String constraintsOracle() {
		return null;
	}

	@Override
	public String indexesMSSQL() {
		return "OMOP CDM sql server pk indexes.txt";
	}

	@Override
	public String indexesPostgreSQL() {
		return "OMOP CDM postgresql pk indexes.txt";
	}

	@Override
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
		return "OMOP CDM Results sql server pk indexes.txt";
	}

	@Override
	public String resultsIndexesPostgreSQL() {
		return "OMOP CDM Results postgresql pk indexes.txt";
	}

	@Override
	public String resultsIndexesOracle() {
		return "OMOP CDM Results oracle pk indexes.txt";
	}

}
