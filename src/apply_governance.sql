-- Databricks notebook source

-- Capture the parameter from the Bundle (Defaults to dev if run manually)
CREATE WIDGET TEXT env_catalog DEFAULT 'dev_bootcamp';
USE CATALOG IDENTIFIER(:env_catalog);
USE SCHEMA data_ingestion;

-- Column Masking Policy Function 
CREATE OR REPLACE FUNCTION name_mask_policy(name STRING)
RETURN CASE 
  WHEN is_account_group_member('admin') THEN name 
  ELSE 'REDACTED_PII' 
END;

-- Row Filtering Policy Function
CREATE OR REPLACE FUNCTION office_filter_policy(office STRING)
RETURN is_account_group_member('admin') OR office = 'RIO';

-- Bind the security policies to the PySpark Table
ALTER TABLE person_standard_ok 
ALTER COLUMN name SET MASK name_mask_policy;

ALTER TABLE person_standard_ok 
SET ROW FILTER office_filter_policy ON (office);

ALTER TABLE person_standard_ko 
ALTER COLUMN name SET MASK name_mask_policy;

ALTER TABLE person_standard_ko 
SET ROW FILTER office_filter_policy ON (office);