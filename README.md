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

Use the flags given below as necessary depending on if you are running the load_balancer on the VM,
in Docker, or locally.

Go to the client -folder
run: 
     
     python3 client_main.py [ -vm | -docker ]

#### Server

Go to the server -folder
run: 
    
     python3 server_main.py [ -vm | -docker ]

#### Load balancer

The load balancer is assumed to run on the VM machine called Client with IP address 192.168.56.101. 

When running the load_balancer, the following environment variables can be used to determine the address and port of the Kafka instance:

| Variable | Use case |
| -------- | -------- |
| vm       | When running the Kafka instance in a VM |
| production | When running the Kafka instance in a VM |
| docker | When running the Kafka instance with the provided docker-compose.yml |

If you are running the Kafka instance on the local machine, you do not need to provide this environment variable.

Go to the load_balancer -folder
run: 

     export FLASK_APP=load_balancer.py
     export ENV=<the variable from the table above that suits the environment>
    
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
### Briefly try out with support of multiple topics

A special topic "**balancer-special**" is defined. It can be a topic used by the load balancer and one server to communicate or shared by all servers to communicate with the load balancer. (I prefer the former solution) The script *server/temp_add_topic.py* is just a temporary script for testing.

To try out the new feature:

1. Run the *server_game.py* directly.
2. If you want, run two *client_game.py* and possibly play the game as usual.
3. Run the *temp_add_topic.py* and enter some unique topic names to add
4. Choose any one of the new topics you just created, and manually replace the fixed topic name at the bottom of *client_game.py* with it. Then run two *client_game.py* now, and you can see the game also goes well, which means the system now supports adding new topics and playing in those topics.

