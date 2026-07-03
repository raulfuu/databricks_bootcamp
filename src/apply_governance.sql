-- Databricks notebook source

-- Column Masking Policy Function 
CREATE OR REPLACE FUNCTION dev_bootcamp.data_ingestion.name_mask_policy(name STRING)
RETURN CASE 
  WHEN is_account_group_member('admin') THEN name 
  ELSE 'REDACTED_PII' 
END;

-- Row Filtering Policy Function
CREATE OR REPLACE FUNCTION dev_bootcamp.data_ingestion.office_filter_policy(office STRING)
RETURN is_account_group_member('admin') OR office = 'RIO';

-- Bind the security policies to the DLT Materialized View
ALTER MATERIALIZED VIEW dev_bootcamp.data_ingestion.person_standard_ok 
ALTER COLUMN name SET MASK dev_bootcamp.data_ingestion.name_mask_policy;

ALTER MATERIALIZED VIEW dev_bootcamp.data_ingestion.person_standard_ok 
SET ROW FILTER dev_bootcamp.data_ingestion.office_filter_policy ON (office);