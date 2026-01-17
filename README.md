# Rearc Data Quest – AWS Data Pipeline - Lekhya Submission

This repository contains my solution for the **Rearc Data Quest**, implementing an end-to-end data pipeline using AWS services and Infrastructure as Code.

The project demonstrates data ingestion, storage, analytics, and automation using **AWS CDK, Lambda, S3, SQS, and Pandas**.

---

## High-Level Overview

The pipeline is built in four parts:

1. **Part 1 – Data Sourcing (BLS → S3)**  
   Republish Bureau of Labor Statistics (BLS) time-series data into Amazon S3 while complying with BLS access policies.

2. **Part 2 – API Integration (Population API → S3)**  
   Fetch US population data from a public API and store the response as JSON in S3.

3. **Part 3 – Data Analytics**  
   Perform analytics using Pandas to generate required reports:
   - Mean & standard deviation of US population (2013–2018)
   - Best year per `series_id` based on summed quarterly values
   - Join time-series data with population data for a specific series and period

4. **Part 4 – Automation & Infrastructure (AWS CDK)**  
   Automate the pipeline using AWS CDK:
   - Scheduled ingestion Lambda
   - S3 → SQS notification
   - Report Lambda triggered by SQS messages

---

## Architecture Summary

- **Amazon S3**  
  Stores republished BLS datasets and population API output

- **AWS Lambda**
  - Ingest Lambda: runs Part 1 and Part 2
  - Report Lambda: runs Part 3 analytics and logs results
    
- **Amazon SageMaker**
   - Part 3  Data Analysis and reports
     
- **Amazon SQS**  
  Receives messages when population data is written to S3

- **AWS CDK (TypeScript)**  
  Defines and deploys all infrastructure

---
```text
## Repository Structure
├── bin/ # CDK app entry point
├── lib/ # CDK stack definitions
├── lambda/
│ ├── ingest/ # Lambda for Part 1 & Part 2
│ └── reports/ # Lambda for Part 3 analytics
├── submission/
│ ├── notebooks/
│ │ └── analysis.ipynb # Part 3 analysis notebook
│ ├── outputs/ #saved outputs and logs
| ├── screenshots/
│   ├── AWS_S3Bucket.png
│   ├── CloudWatchReportLogs.png
│   └── ReportLambda.png
│ 
├── README.md
├── cdk.json
├── package.json
└── tsconfig.json

```
---

## How to Deploy and Run

### Prerequisites

- AWS account with permissions for:
  - S3, Lambda, SQS, IAM, CloudFormation, EventBridge
- Node.js and npm
- AWS CDK v2
- AWS CLI configured

---

### Install Dependencies

npm install

-----------
Deploy
npx cdk deploy
--------------
After deployment, the stack outputs:

S3 bucket name: rearcquestcdkstack-questbucket7af4fe29-mz5fzkoo95r

SQS queue URL: https://sqs.us-east-1.amazonaws.com/509010658370/RearcQuestCdkStack-PopulationQueue6729BE7E-vp1WIifjNrMX

Verify Data in S3
aws s3 ls s3://<BUCKET_NAME>/bls/ --recursive

## Trigger Report Lambda
aws lambda invoke \
  --function-name "<ReportLambdaName>" \
  --payload '{}' \
  /tmp/report_out.json

cat /tmp/report_out.json

------------
## View Analytics Output
aws logs tail "/aws/lambda/<ReportLambdaName>" --since 30m
--------------------------

## Part 3 – Data Analytics Details

- The notebook submission/notebooks/analysis.ipynb contains:

- Data loading and cleaning

- Population statistics (mean & standard deviation for 2013–2018)

- Best year per series_id based on summed quarterly values

- Joined report for series_id = PRS30006032 and period = Q01

- The same logic is reused in the Report Lambda for automated execution.

## Part 4 – Automation with AWS CDK
- Infrastructure is defined using AWS CDK (TypeScript)
- Includes:
  - Lambda for ingestion (Part 1 & 2)
  - S3 notification → SQS
  - Lambda triggered by SQS to run analytics (Part 3)
  - Scheduled daily execution

## Notes

- Analytics results are logged to CloudWatch Logs

- S3 bucket and SQS queue are created via CDK

- Lambda functions are fully automated

- AI tools were used as a reference and learning aid; all logic is understood and explainable





