DROP TABLE IF EXISTS @target_schema.concept;
DROP TABLE IF EXISTS @target_schema.concept_ancestor;
DROP TABLE IF EXISTS @target_schema.concept_class;
DROP TABLE IF EXISTS @target_schema.concept_relationship;
DROP TABLE IF EXISTS @target_schema.concept_synonym;
DROP TABLE IF EXISTS @target_schema.domain;
DROP TABLE IF EXISTS @target_schema.drug_strength;
DROP TABLE IF EXISTS @target_schema.relationship;
DROP TABLE IF EXISTS @target_schema.source_to_concept_map;
DROP TABLE IF EXISTS @target_schema.vocabulary;

CREATE TABLE @target_schema.concept AS 
SELECT * FROM @vocab_schema.concept;

CREATE TABLE @target_schema.concept_ancestor AS 
SELECT * FROM @vocab_schema.concept_ancestor;

CREATE TABLE @target_schema.concept_class AS 
SELECT * FROM @vocab_schema.concept_class;

CREATE TABLE @target_schema.concept_relationship AS 
SELECT * FROM @vocab_schema.concept_relationship;

CREATE TABLE @target_schema.concept_synonym AS 
SELECT * FROM @vocab_schema.concept_synonym;

CREATE TABLE @target_schema.domain AS 
SELECT * FROM @vocab_schema.domain;

CREATE TABLE @target_schema.drug_strength AS 
SELECT * FROM @vocab_schema.drug_strength;

CREATE TABLE @target_schema.relationship AS 
SELECT * FROM @vocab_schema.relationship;

CREATE TABLE @target_schema.source_to_concept_map AS 
SELECT * FROM @vocab_schema.source_to_concept_map;

CREATE TABLE @target_schema.vocabulary AS 
SELECT * FROM @vocab_schema.vocabulary;


