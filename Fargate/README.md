# Create Python Script to be run in AWS Fargate
- The goal of this project is to have a python script runnable inside of a container runtime platform.
- These scripts may contain dependencies too large for Lambda, and also may run longer than the allowed lambda runtime limits.

## Developing the application

1. Placeholder [`script.py`](./script.py) annd [`requirements.txt`](./requirements.txt) files have been created. Extend these as you wish.

2. If you wish to rename or add additonal `.py` files you must modify the [`Dockerfile`](./Dockerfile)

## Building the Docker Image

1. Ensure Docker is installed on your machine. If not, you can download and install from [Docker's official website](https://www.docker.com/products/docker-desktop).

2. Open a terminal in the project root directory (where the Dockerfile is located).

3. Build the Docker image by running the following command:

```sh
docker build -t your_image_name .
```

4. After the build process is complete, you can verify the image was created successfully by running:

```sh
docker image ls
```

You should see `your_image_name` in the list of available Docker images.

## Running the Docker Image Locally

To run a container from your image, use the following command:

```sh
docker run -rm your_image_name
```

## Pushing the container to ECR (Elastic Container Registry)

1. Log into AWS. 

```sh
aws configure
```
- When prompted use Use region `us-east-1`

2. Create the ECR Repository

```sh
aws ecr create-repository --repository-name my-repository
```
- Name the repository something related to what the code does.

3. Authinticate Docker to your ECR Repository
```sh
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <<account id>>.dkr.ecr.us-east-1.amazonaws.com
```
4. Tag the image created above
```sh
docker tag your_image_name:latest <<account id>>.dkr.ecr.us-east-1.amazonaws.com/my-repository:latest
```
- Note the name of the image name and the repository name will differ.

5. Push the image created above to the Repository
```sh
docker push <<account id>>.dkr.ecr.us-east-1.amazonaws.com/my-repository:latest
```

## Create Fargate Task Definition
- Sample task definition is available [here](./task_definition.json)
- Change the Image to your image created above
- If you use boto3 you will need to create an execution role with the necessary permissions
- Make sure to include the `"logDriver": "awslogs"` if you would like your standard output available in Cloudwatch
```sh
aws ecs register-task-definition --cli-input-json file://task_definition.json
```

## Create Lambda function to start the container when an S3 object is created
TODO

## Getting the logs from AWS Fargate
Get the log streams for our log group:
```sh
aws logs describe-log-streams --log-group-name /ecs/test-task
```
Get the logs from that log stream
```sh
aws logs get-log-events --log-group-name /ecs/test-task --log-stream-name logstreamname
```
Only get the messages
```sh
aws logs get-log-events --log-group-name /ecs/test-task --log-stream-name logstreamname | jq '.events[].message'
```