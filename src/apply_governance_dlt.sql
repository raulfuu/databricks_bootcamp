-- Databricks notebook source

CREATE WIDGET TEXT env_catalog DEFAULT 'dev_bootcamp';
USE CATALOG IDENTIFIER(:env_catalog);
USE SCHEMA data_ingestion;

CREATE OR REPLACE FUNCTION name_mask_policy(name STRING)
RETURN CASE 
  WHEN is_account_group_member('admin') THEN name 
  ELSE 'REDACTED_PII' 
END;

CREATE OR REPLACE FUNCTION office_filter_policy(office STRING)
RETURN is_account_group_member('admin') OR office = 'RIO';

-- Bind the security policies to the DLT Materialized View
ALTER MATERIALIZED VIEW person_standard_ok_dlt 
ALTER COLUMN name SET MASK name_mask_policy;

ALTER MATERIALIZED VIEW person_standard_ok_dlt 
SET ROW FILTER office_filter_policy ON (office);

ALTER MATERIALIZED VIEW person_standard_ko_dlt 
ALTER COLUMN name SET MASK name_mask_policy;

ALTER MATERIALIZED VIEW person_standard_ko_dlt 
SET ROW FILTER office_filter_policy ON (office);