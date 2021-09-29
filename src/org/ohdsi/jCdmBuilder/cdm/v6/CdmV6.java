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
		return "OMOP CDM sql server constraints.txt";
	}

	@Override
	public String constraintsPostgreSQL() {
		return "OMOP CDM postgresql constraints.txt";
	}

	@Override
	public String constraintsOracle() {
		return "OMOP CDM oracle constraints.txt";
	}

	@Override
	public String indicesMSSQL() {
		return "OMOP CDM sql server pk indices.txt";
	}

	@Override
	public String indicesPostgreSQL() {
		return "OMOP CDM postgresql pk indices.txt";
	}

	@Override
	public String indicesOracle() {
		return "OMOP CDM oracle pk indices.txt";
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
	public String resultsIndicesMSSQL() {
		return "OMOP CDM Results sql server pk indexes.txt";
	}

	@Override
	public String resultsIndicesPostgreSQL() {
		return "OMOP CDM Results postgresql pk indexes.txt";
	}

	@Override
	public String resultsIndicesOracle() {
		return "OMOP CDM Results oracle pk indexes.txt";
	}

}
