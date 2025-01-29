# S3 File Upload Tracker

## Overview
This application monitors and calculates the number of files uploaded to an Amazon S3 bucket per hour. It leverages AWS CloudTrail and Amazon Athena to track file uploads efficiently. The results can be analyzed to understand file transfer patterns and optimize storage operations.

## Functionality
- Tracks file uploads to S3 buckets.
- Supports single-region and multi-region monitoring.
- Uses AWS CloudTrail to log S3 events.
- Processes event data with Amazon Athena for analysis.
- Provides aggregated hourly upload counts.

## Prerequisites
- An AWS account with permissions to configure CloudTrail, S3, and Athena.
- An existing S3 bucket to store CloudTrail events.
- AWS CLI installed and configured.
- Athena enabled in the AWS account.

## Parameters

### `ControlTower`
- **Type:** String
- **Description:** Specifies if monitoring should be done at the regional level.
- **Allowed Values:**
  - `true` - Enables Control Tower monitoring.
  - `false` - Disables Control Tower monitoring.

### `MultiRegion`
- **Type:** String
- **Description:** Indicates whether the application should monitor buckets across multiple regions.
- **Allowed Values:**
  - `true` - Enables multi-region support.
  - `false` - Monitors a single region.

### `TrailBucket`
- **Type:** String
- **Description:** The name of the S3 bucket where CloudTrail events will be stored.
- **Default Value:** `filestorage-events-trendmicro`

### `AthenaBucket`
- **Type:** String
- **Description:** The name of the S3 bucket where Athena query results will be stored.
- **Default Value:** `athena-results-trendmicro`

### `OrganizationID`
- **Type:** String
- **Description:** The AWS Organization ID, required when using Control Tower.

## Installation
1. Clone the repository:
   ```sh
   git clone https://github.com/your-repo/s3-file-upload-tracker.git
   cd s3-file-upload-tracker
   ```
2. Configure the required AWS services (CloudTrail, Athena, and S3).
3. Deploy the application using AWS CloudFormation

## Usage
1. Ensure CloudTrail is logging S3 events.
2. Run queries in Athena to analyze the data.
3. Extract hourly upload statistics from the query results.

## Troubleshooting
- **No file upload data is being collected:** Ensure CloudTrail is enabled and correctly configured.
- **Athena queries are failing:** Verify that the Athena query results bucket exists and has the correct permissions.
- **Permission issues:** Ensure the AWS IAM roles have necessary permissions to read S3 logs and execute Athena queries.

## License
This project is licensed under the MIT License.

