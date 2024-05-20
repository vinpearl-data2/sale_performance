## I. Installation

## Step 1. Install miniconda (already in server)

Conda is kind of python environment management.

If you want to install on local, please following this intrustion https://docs.conda.io/en/main/miniconda.html

For windows just download and run exe file. For linux/Mac download and run file *.sh on your terminal




## Step 2. Install dbt-biguqery

2.1. Open terminal

2.2. Create new conda environment (if not exist)

conda create -n <your_env_name> python=<version_you_want>

Ex: conda create -n dbt python=3.8

2.3. Active conda env

conda activate <your_env_name>

Ex: conda activate dbt

2.4. install dbt-bigquery

pip install dbt-bigquery

## Step 3. Copy this template in to anyplace you want

## II. Active google account.

## Step 1: Install gcloud cli by follow the gcloud's website

## Step 2: Active by run on terminal

gcloud auth application-default login

gcloud auth login

## II. Run new project

## For any dbt project, there are 3 configs you need to have

## 1. dbt_project.yml

See template for detail information
```
models:
  <dbt_project_name>:
    <first_level_path>:
      project: <bigquery_project_name>
      <second_level_path>:
        materialized: <view/table>
        schema: <bigquery_table_name>
        tags: <tag_name>
```

## 2. profiles.yml

See template for detail information

```
<dbt_project_name>:
  outputs:
    prod:
      schema: <header_bigquery_project_name>
      job_execution_timeout_seconds: 300
      job_retries: 1
      location: "asia-southeast1"
      method: oauth
      project: vp-dwh-prod-c827
      threads: 10
      type: bigquery
      priority: batch
  target: prod
```

## 3. Config block

Config block will replace any config in dbt_project.yml. See any *.sql example in models for any information.

## III. Run

## Step 1: Chmod for dbt_run.sh

chmod +x dbt_run.sh

## Step 2: Run

./dbt_run.sh

## Step 3: Show results

dbt docs generate

dbt docs serve