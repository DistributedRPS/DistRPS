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
If you have trouble with any of these Python commands, you might need to activate the Virtual environment (venv) for the project in question.
The venv is activated at the root of the node folder (for example, in the 'client' -folder) by running:

     . bin/activate
     
After activating the venv, when you are done with running the apps, you can deactivate it by simply running:

     deactivate
     
To install the dependencies, run:
     
     pip3 install -r requirements.txt

Each VM has as password 123. It makes uses of Lubuntu, expects around 13 gigs of storage space and 1 gig of RAM.
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

The load balancer is assumed to run on the VM machine called Client with IP address 192.168.56.101. 

Go to the load_balancer -folder
run: 

     export FLASK_APP=load_balancer.py
    
     flask run --host=0.0.0.0

#### Kafka node and kafka zookeeper

The kafka node is assumed to run on the VM called Server with IP address 192.168.56.103.
Only the load balancer is aware of the IP address of the kafka node, the game servers and clients need to request it from the load balancer.
To run the kafka and zookeeper services, please run the startKafka.sh script. To check if the kafka service is properly running, try the command sudo systemctl status kafka.
To stop the kafka and zookeeper service, please run the stopKafka.sh script.

See kafka documentation for more information.

### Creating local images

Go to each components folder, where the corresponding Dockerfile is located and run:

     docker build -t <desired name for image> .



## Run game logic example

There are two scripts: client_game.py in client folder and server_game.py in server folder.

```
docker cp path container_name:/app
```

Use this copy command to copy the scripts into the containers.

To go into the container command line:

```
docker exec -it container_name /bin/bash
```

Run the scripts in different terminal in any nodes with python environment. (consumer or producer, it doesn't matter. Anyway they are different processes.)

First run the server. After several seconds (to wait for it to set up), run the two clients.

Right now it doesn't have any fault tolerance, but the basic logic is done. 3 rounds, 2 players.
