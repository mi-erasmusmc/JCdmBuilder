BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE @target_schema.concept;
  EXECUTE IMMEDIATE 'DROP TABLE @target_schema.concept;
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;

BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE @target_schema.concept_ancestor;
  EXECUTE IMMEDIATE 'DROP TABLE @target_schema.concept_ancestor;
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;

BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE @target_schema.concept_class;
  EXECUTE IMMEDIATE 'DROP TABLE @target_schema.concept_class;
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;

BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE @target_schema.concept_relationship;
  EXECUTE IMMEDIATE 'DROP TABLE @target_schema.concept_relationship;
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;

BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE @target_schema.concept_synonym;
  EXECUTE IMMEDIATE 'DROP TABLE @target_schema.concept_synonym;
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;

BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE @target_schema.domain;
  EXECUTE IMMEDIATE 'DROP TABLE @target_schema.domain;
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;

BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE @target_schema.drug_strength;
  EXECUTE IMMEDIATE 'DROP TABLE @target_schema.drug_strength;
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;

BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE @target_schema.relationship;
  EXECUTE IMMEDIATE 'DROP TABLE @target_schema.relationship;
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;

BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE @target_schema.source_to_concept_map;
  EXECUTE IMMEDIATE 'DROP TABLE @target_schema.source_to_concept_map;
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;

BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE @target_schema.vocabulary;
  EXECUTE IMMEDIATE 'DROP TABLE @target_schema.vocabulary;
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
      RAISE;
    END IF;
END;

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


