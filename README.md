# Rearc Data Quest – AWS Data Pipeline  
**Submission by Lekhya**

This repository contains my submission for the **Rearc Data Quest**.  
The goal of this project is to build a simple but complete **AWS-based data pipeline** that demonstrates data ingestion, storage, analytics, and automation.

The solution uses **AWS Lambda, S3, SQS, CloudWatch, SageMaker, and AWS CDK (TypeScript)**.

---

## Overview

This pipeline:

- Republishes BLS time-series data into Amazon S3  
- Fetches US population data from a public API  
- Performs analytics using Pandas  
- Automates execution using AWS CDK, Lambda scheduling, and SQS triggers  

---

## Quest Parts Breakdown

### Part 1 – AWS S3 & Data Sourcing

- Republished Bureau of Labor Statistics (BLS) datasets into Amazon S3
- Programmatic access follows BLS policy using a User-Agent header
- Prevents duplicate uploads of unchanged files

**S3 Output**
```
s3://rearcquestcdkstack-questbucket7af4fe29-mz5fzkoo95ri/bls/pr/pr.data.0.Current
```

---

### Part 2 – API Integration

- Fetches US population data from a public API
- Stores the response as JSON in Amazon S3

**S3 Output**
```
s3://rearcquestcdkstack-questbucket7af4fe29-mz5fzkoo95ri/bls/api/population.json
```

---

### Part 3 – Data Analytics

Analytics were implemented using **Pandas** and validated in **Amazon SageMaker**.

Reports generated:

- Mean and standard deviation of US population (2013–2018)
- Best year per `series_id` based on summed quarterly values
- Joined report for:
  - `series_id = PRS30006032`
  - `period = Q01`
  - Population for the same year

The full analysis and results are available here:

```
submission/notebooks/analysis.ipynb
```

---

### Part 4 – Automation & Infrastructure (AWS CDK)

Infrastructure is fully automated using **AWS CDK (TypeScript)**.

Includes:

- Ingest Lambda (Part 1 & 2)
- S3 → SQS notification
- Report Lambda triggered by SQS
- CloudWatch logging
- Daily scheduled execution

---

## Architecture Summary

- **Amazon S3** – Stores datasets and API outputs  
- **AWS Lambda**
  - Ingest Lambda (Part 1 & 2)
  - Report Lambda (Part 3 analytics)
- **Amazon SQS** – Triggers analytics when new data arrives
- **Amazon SageMaker** – Interactive analysis
- **AWS CDK** – Infrastructure as Code

---

## Repository Structure

```
├── bin/                     # CDK app entry point
├── lib/                     # CDK stack definitions
├── lambda/
│   ├── ingest/              # Part 1 & 2 Lambda
│   └── reports/             # Part 3 analytics Lambda
├── submission/
│   ├── notebooks/
│   │   └── analysis.ipynb
│   ├── outputs/
│   ├── screenshots/
│   │   ├── AWS_S3Bucket.png
│   │   ├── CloudWatchReportLogs.png
│   │   ├── SQS_Queue.png
│   │   └── ReportLambda.png
├── README.md
├── cdk.json
├── package.json
└── tsconfig.json
```

---

## Deployment

### Prerequisites

- AWS account
- AWS CLI configured
- Node.js & npm
- AWS CDK v2

### Install Dependencies

```bash
npm install
```

### Deploy Infrastructure

```bash
npx cdk deploy
```

### Stack Outputs

- **S3 Bucket**
```
rearcquestcdkstack-questbucket7af4fe29-mz5fzkoo95ri
```

- **S3 Bucket URL**

```
p1- BLS dataset S3:
https://rearc-bls-republish-2026.s3.us-east-1.amazonaws.com/bls/pr/pr.data.0.Current

p2 - Population S3:
https://rearc-bls-republish-2026.s3.us-east-1.amazonaws.com/bls/api/population.json

```
- **SQS Queue Endpoint**
```
https://sqs.us-east-1.amazonaws.com/509010658370/RearcQuestCdkStack-PopulationQueue6729BE7E-vp1WIifjNrMX
```

---

## Verification

### Verify S3 Data

```bash
aws s3 ls s3://rearcquestcdkstack-questbucket7af4fe29-mz5fzkoo95ri/bls/ --recursive
```

### Trigger Report Lambda

```bash
aws lambda invoke \
  --function-name "RearcQuestCdkStack-ReportLambda9FE277D8-dSK0xpM9rlrl" \
  --payload '{}' /tmp/report_out.json

cat /tmp/report_out.json
```

### View Logs

```bash
aws logs tail "/aws/lambda/RearcQuestCdkStack-ReportLambda9FE277D8-dSK0xpM9rlrl" --since 30m
```

---

## Notes

- Analytics output is logged to CloudWatch
- Infrastructure is fully automated via AWS CDK
- AI tools were used as a learning/reference aid; all logic is understood and explainable

---

