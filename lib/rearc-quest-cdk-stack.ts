import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as sources from "aws-cdk-lib/aws-lambda-event-sources";
import * as path from "path";

export class RearcQuestCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const PR_CURRENT_KEY = "bls/pr/pr.data.0.Current";
    const POP_S3_KEY = "bls/api/population.json";

    const bucket = new s3.Bucket(this, "QuestBucket", {
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const queue = new sqs.Queue(this, "PopulationQueue", {
      visibilityTimeout: cdk.Duration.seconds(360),
    });

    const ingest = new lambda.Function(this, "IngestLambda", {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: "index.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/ingest")),
      timeout: cdk.Duration.minutes(10),
      memorySize: 512,
      environment: {
        S3_BUCKET: bucket.bucketName,
        POP_S3_KEY: POP_S3_KEY,
      },
    });

    bucket.grantReadWrite(ingest);

    new events.Rule(this, "DailyIngestRule", {
      schedule: events.Schedule.rate(cdk.Duration.days(1)),
      targets: [new targets.LambdaFunction(ingest)],
    });

    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SqsDestination(queue),
      { prefix: "bls/api/", suffix: "population.json" }
    );

    const report = new lambda.DockerImageFunction(this, "ReportLambda", {
      code: lambda.DockerImageCode.fromImageAsset(
        path.join(__dirname, "../lambda/reports")
      ),
      timeout: cdk.Duration.minutes(5),
      memorySize: 512,
      environment: {
        S3_BUCKET: bucket.bucketName,
        PR_CURRENT_KEY: PR_CURRENT_KEY,
        POP_S3_KEY: POP_S3_KEY,
        SERIES_ID: "PRS30006032",
        PERIOD: "Q01",
      },


    });

    bucket.grantRead(report);
    queue.grantConsumeMessages(report);
    report.addEventSource(new sources.SqsEventSource(queue, { batchSize: 1 }));

    new cdk.CfnOutput(this, "BucketName", { value: bucket.bucketName });
    new cdk.CfnOutput(this, "QueueUrl", { value: queue.queueUrl });
  }
}
