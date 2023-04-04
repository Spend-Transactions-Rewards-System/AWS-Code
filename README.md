<!-- PROJECT LOGO -->
<br />
<div align="center">
<h3 align="center">ITSA G1 T3 Project B AY2022/23 Semester 2</h3>

  <p align="center">
    AWS resources: Glue, lambda
    <br />
    <a href="https://www.itsag1t3.com">View Demo</a>
  </p>
</div>


<!-- ABOUT THE PROJECT -->
## About This Repository
This repository provides the necessary codes and setup guides for AWS Glue and Lambda. 


<!-- GETTING STARTED -->
## Setup
### AWS Glue
1. Set up connectors for your private database. As we are not using AWS Glue built-in pyspark transformation framework *(DynamicFrames)*, the connectors act mainly as a bridge for us to access our private resources in our private subnet. AWS Glue sets up elastic network interfaces that enable our jobs to connect securely to other resources within our VPC. 
    - Do ensure that vpc, subnet and security groups pertaining to the resources are selected during the set-up.
2. Ensure that your security groups allow `itself` to access all TCP ports. 
3. Upload mysql connector wheel file to s3 bucket. File can found [here](./package/mysql_connector_python-8.0.32-py2.py3-none-any.whl). Include the s3 path to `Python library path` when creating a spark job.
4. - scripts can be found in this [folder](./Glue/).

## AWS Lambda
- scripts can be found in this [folder](./lambda/).


<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

### Team Members
* Sean Tan
* Joshua Zhang
* Tan Gi Han
* Stanford
* Hafil
* Dino
* Gan Shao Hong

### Project Advisor/Mentor
* [Professor Ouh Eng Lieh](https://www.linkedin.com/in/eng-lieh-ouh/?originalSubdomain=sg)

