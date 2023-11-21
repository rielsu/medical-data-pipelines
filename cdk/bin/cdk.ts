import 'source-map-support/register'
import * as cdk from 'aws-cdk-lib'
import { OneUpHealthDataPipelineStack } from '../lib/pipelines/oneuphealth-stack'
import { DataStack } from '../lib/data-stack'
import { type StageConfig, getConfig } from '../lib/config-builder'
import { OneUpHealthDataCatalogStack } from '../lib/data-catalog-stack'
import { EventsStack } from '../lib/events-stack'

// Initialize a new CDK app instance.
const app = new cdk.App()

// Retrieve configuration for the deployment stage.
const stageConfig: StageConfig = getConfig(app)

// Instantiate a Data Stack for the Data Lake.
const dataLakeStack = new DataStack(app, ' medicalDataPipelinesDataStack', stageConfig, {
  healthDataApiAwsAccountId: stageConfig.healthDataApiAwsAccountId,
  env: {
    account: stageConfig.awsAccountId,
    region: stageConfig.awsRegion,
  },
})

// Instantiate the OneUp Health Data Pipeline Stack.
const oneUpHealthDataPipelineStack = new OneUpHealthDataPipelineStack(
  app,
  ' medicalDataPipelinesOneUpHealthDataPipelineStack',
  stageConfig,
  {
    dataLakeBucket: dataLakeStack.dataLakeBucket,
    eventBridgeSourceAccount: stageConfig.platformAwsAccountId,
    env: {
      account: stageConfig.awsAccountId,
      region: stageConfig.awsRegion,
    },
  },
)

// Instantiate the OneUp Health Data Catalog Stack.
const oneUpHealthDataCatalogStack = new OneUpHealthDataCatalogStack(
  app,
  ' medicalDataPipelinesOneUpHealthDataCatalogStack',
  stageConfig,
  {
    dataStack: dataLakeStack,
    healthDataApiAwsAccountId: stageConfig.healthDataApiAwsAccountId,
    env: {
      account: stageConfig.awsAccountId,
      region: stageConfig.awsRegion,
    },
  },
)

// Instantiate the Events Stack.
const eventsStack = new EventsStack(app, ' medicalDataPipelinesEventsStack', stageConfig, {
  platformAwsAccountId: stageConfig.platformAwsAccountId,
  plarformEventsSoruceNames: stageConfig. medicalEventsSourceNames,
  oneUpHealthDataPipelineStateMachine:
    oneUpHealthDataPipelineStack.oneUpHealthDataPipelineStateMachine,
  env: {
    account: stageConfig.awsAccountId,
    region: stageConfig.awsRegion,
  },
})

// Define dependencies between the stacks to ensure they are deployed in the correct order.
oneUpHealthDataPipelineStack.addDependency(dataLakeStack)
oneUpHealthDataCatalogStack.addDependency(dataLakeStack)
eventsStack.addDependency(oneUpHealthDataPipelineStack)
