# CDPtoAWS
This repo show how can load data from Hive table in Cloudera Data Platform (CDP) to Amazon Web Services (AWS).<br />
I have split script for 2 parts as is my folder..<br />
<br />
### CDP_part 
This part is load data from Hive table and save it to csv file and put it to AWS S3.
CDP part is running on Clodera Data Science Workbench (CDSW) which is a platform provided by Cloudera for data scientists to develop, run, and manage data science projects. It is designed to work with Cloudera Data Platform (CDP), which is a comprehensive data management and analytics platform. more (https://www.cloudera.com/products/data-science-and-engineering/data-science-workbench.html).<br />
CDSW is applied to data orchestration tools and monitoring tool.
<br />
### AWS_part 
This part is load data from AWS S3 to RDS which is web application database.Script is running on AWS glue and workflow.Workflow is run when data is uploaded to S3 bucket.<br />
Workflow is triggered by AWS Eventbridge.
