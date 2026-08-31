# E-Commerce Data Quality Validation Pipeline

## Project Overview

This project is an automated data quality validation pipeline developed for e-commerce transaction data. The pipeline uses PySpark to process transaction records and identify data quality issues before the data is used for downstream analytics and machine learning activities.

The pipeline applies a set of validation rules to check the quality, completeness, consistency, uniqueness, and validity of transaction data. Based on the validation results, records are classified as Valid, Invalid, or Anomaly.

## Tech Stack

The project is developed using Python and PySpark for data processing and validation. YAML is used for configuration management so that important paths and validation thresholds can be maintained separately from the code. Pytest is used for unit testing, Python logging is used for tracking pipeline execution and errors, and an HTML report is generated to provide a summary of the data quality results.

## Data Quality Validation

The pipeline implements 10 data quality rules covering:

* Schema and required column validation
* Transaction ID uniqueness and null validation
* Domain value validation
* Customer and merchant ID validation
* Transaction amount range validation
* Transaction date validation
* Logical consistency between transaction fields
* Statistical outlier detection using mean and standard deviation
* Column-level completeness checks
* Data freshness checks

Rules 2–7 are mandatory validation rules used to identify Invalid records, while Rules 8–10 are informational checks used to identify unusual or anomalous records.

## Output

After validation, the pipeline separates the processed data into Valid, Invalid, and Anomaly records. It also calculates an overall data quality score and generates an HTML quality report containing record counts, rule-level results, data profiling information, and recommendations.

The pipeline also maintains execution logs and provides unit tests to verify the validation logic.

## Objective

The main objective of this project is to build a simple and maintainable data quality pipeline that can automatically detect data issues, prevent invalid records from reaching downstream systems, and provide clear visibility into the overall quality of the transaction data.
