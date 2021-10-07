DROP TABLE @target_schema.concept;
DROP TABLE @target_schema.concept_ancestor;
DROP TABLE @target_schema.concept_class;
DROP TABLE @target_schema.concept_relationship;
DROP TABLE @target_schema.concept_synonym;
DROP TABLE @target_schema.domain;
DROP TABLE @target_schema.drug_strength;
DROP TABLE @target_schema.relationship;
DROP TABLE @target_schema.source_to_concept_map;
DROP TABLE @target_schema.vocabulary;

SELECT * 
INTO @target_schema.concept
FROM @vocab_schema.concept;

SELECT * 
INTO @target_schema.concept_ancestor
FROM @vocab_schema.concept_ancestor;

SELECT * 
INTO @target_schema.concept_class
FROM @vocab_schema.concept_class;

SELECT * 
INTO @target_schema.concept_relationship
FROM @vocab_schema.concept_relationship;

SELECT * 
INTO @target_schema.concept_synonym
FROM @vocab_schema.concept_synonym;

SELECT * 
INTO @target_schema.domain
FROM @vocab_schema.domain;

SELECT * 
INTO @target_schema.drug_strength
FROM @vocab_schema.drug_strength;

SELECT * 
INTO @target_schema.relationship
FROM @vocab_schema.relationship;

SELECT * 
INTO @target_schema.source_to_concept_map
FROM @vocab_schema.source_to_concept_map;

SELECT * 
INTO @target_schema.vocabulary
FROM @vocab_schema.vocabulary;

ALTER TABLE @target_schema.concept REBUILD WITH ( DATA_COMPRESSION = PAGE );
ALTER TABLE @target_schema.concept_ancestor REBUILD WITH ( DATA_COMPRESSION = PAGE );
ALTER TABLE @target_schema.concept_relationship REBUILD WITH ( DATA_COMPRESSION = PAGE );
ALTER TABLE @target_schema.concept_synonym REBUILD WITH ( DATA_COMPRESSION = PAGE );
ALTER TABLE @target_schema.domain REBUILD WITH ( DATA_COMPRESSION = PAGE );
ALTER TABLE @target_schema.drug_strength REBUILD WITH ( DATA_COMPRESSION = PAGE );
ALTER TABLE @target_schema.relationship REBUILD WITH ( DATA_COMPRESSION = PAGE );
ALTER TABLE @target_schema.source_to_concept_map REBUILD WITH ( DATA_COMPRESSION = PAGE );
ALTER TABLE @target_schema.vocabulary REBUILD WITH ( DATA_COMPRESSION = PAGE );
