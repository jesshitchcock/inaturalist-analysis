# Introduction
In order ot get this project working, you will need to follow the instructions to set up the AWS Redshift cluster and Airflow in Docker: 

## AWS Set Up Instructions
This project requires an AWS account
Please ensure that the following is configured before attempting to run Airflow: 

#### User Permissions
### IAM User 
Set up an IAM user. This can be done either in the AWS console or the AWS CLI.
Instructions can be found [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html). 

### IAM Roles
Create a role for your IAM user that gives the user access to: 
- AmazonS3FullAccess
- AmazonRedshiftFullAccess
- AmazonRedshiftDataFullAccess

### Policies
Create a policy that allows your IAM user to assume the new role that you created. 

#### AWS Command Line Interface (CLI) 
The AWS CLI enables you to interact with AWS services using commands in your command shell. 
The setup instructions for this can be found [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html).
You can use the AWS CLI to setup a Redshift cluster or you can manually create the cluster in the AWS Console.

### Redshift Cluster 
Before setting up a Redshift cluster you need to choose the region in which to work in. 
The iNaturalist Open Source Image data is located in `us-east-1`, so I chose this region. 

#### VPC, Subnet Group and Security Group
Before setting up a cluster, you must ensure that you have a VPC in your chosen region. 
In addition, you must associate the VPC with a subnet group from the relevant region. 

Once this is set up, you will need to enable traffic to and from the security group associated with your VPC.  

#### Creating the Redshift Cluster 
The Redshift Cluster can be created from the AWS CLI or from the Redshift section in the AWS console. 
As I will reuse the same cluster for the ELT process, I created the cluster prior to the data pipeline:
My cluster was set up as follows: 

**Cluster Configuration**
- Node Type: dc2.large
- Number of Nodes: 4

**Database Configuration**
- Database configurations (added an Admin username and password and saved this, this will be needed later)
- Choose a database name. I chose dev
- Set the port to 5439
- Use default parameter group 
- 
**Cluster Permissions**
- IAM Roles: Associated the cluster with an IAM role that has permissions to access redshift and read from S3. 

**Network and Security**
- Select your VPC and security group (if you cannot find a VPC or security group, this [video](https://www.youtube.com/watch?v=vJY9X-kdd9Q) helps to troubleshoot this issue.)
- Select your subnet group

## Docker Instructions 
This repository uses Airflow 2.2.3 inside Docker. 
I used this [article](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html) to get started with a docker image set up to use with airflow. 

To get airflow running in this repo: 
1. ensure that you have cloned the repository.  
2. Install Airflow according ot the requirements.txt file. 
3. Set up the airflow _AIRFLOW_WWW_USER_USERNAME, _AIRFLOW_WWW_USER_PASSWORD, _PIP_ADDITIONAL_REQUIREMENTS variables, 
by running these commands from the terminal inside the repository: 
`export _AIRFLOW_WWW_USER_USERNAME=airflow`
`export _AIRFLOW_WWW_USER_PASSWORD=airflow`
`export _PIP_ADDITIONAL_REQUIREMENTS= cat requirements.txt`
4. Initialise the Airflow database:
`docker-compose up airflow-init`
5. Run the docker container: 
`docker-compose up`
6. Access the Airflow web interface by navigating to: http://localhost:8080/
7. In the Airflow UI add the following connection variables: 
- **redshift**
	- Amazon Redshift
	- Redshift Cluster Instance (endpoint) e.g. capstone-cluster.csovnx7ncdle.us-east-1.redshift.amazonaws.com
	- Schema: dev
	- Login: username for the redshift cluster user
	- Password: password for the redshift cluster user
	- Port:5439
- **aws_credentials**
	- Conn Type: Amazon Web Services
	- Login IAM credentials Access Key ID
	- Password: IAM secret key ID
7. To turn off the webserver by running:
`docker compose down`
8. If you run into memory issues, update the amount of memory assigned to your docker container: 
   - Inside the Docker UI open "Settings" 
   - Select "Resources" 
   - Update memory to at least 10GB 
   - Select "Apply and Restart"

## Jupyter Notebooks 
In order to run jupyter notebooks you need to create a virtual environment. 
You can do this by following these instructions: 

1. Create a virtualenv: `python3 -m venv {your_virtualenv_name}`
2. Activate your virtualenv: `source {your_virtualenv_name}/bin/activate`
3. Install pip dependencies: `pip install -r requirements.txt`
4. Start jupyter: `jupyter notebook`
5. To shutdown the notebook server use: `ctrl` + `c`
