# DistRPS
A distributed Rock, Paper &amp; Scissors game


## Setting up development environment

### With Docker

There is a docker-compose.yml -file located in the "kafka" -folder of the repository.
In the folder run:

     docker-compose up -d

That should start all the containers connected to a default Docker network so that they are visible to each other.

If the docker-compose.yml fails to start the containers, make sure you have local images of each container, and that they are named as expected by the docker-compose.yml

### Without Docker (components running individually on the local machine)

Make sure you have python3 installed.
You might need to install the dependencies listed in the requirements.txt -folder.

#### Client

Go to the client -folder
run: 
     
     python3 client_main.py

#### Server

Go to the server -folder
run: 

     export FLASK_APP=server_main.py

     flask run

#### Load balancer

Go to the load_balancer -folder
run: 

     export FLASK_APP=load_balancer.py

     flask run

#### Kafka node and kafka zookeeper

See kafka documentation

### Creating local images

Go to each components folder, where the corresponding Dockerfile is located and run: docker build -t <desired name for image> .
(The dot at the end is part of the command!

