# DistRPS
A distributed Rock, Paper &amp; Scissors game


## Setting up the environment

### Using Lubuntu

The components were developed and tested for use on the Lubuntu operating system. Lubuntu is a lightweight form of the more commonly known Ubuntu operating system. More information about Lubuntu and its installation can be found on the following link: https://lubuntu.me/. For our project we made use of Lubuntu 21.10. We gave the virtual machines around 1 gigabyte of RAM and 14 gigabytes of storage space. Two nodes we given a static IP address for use in the development of this project, the Kafka node and the Load Balancer node. The load balancer node is accessible on IP address 192.168.56.101 and the Kafka node is on 192.168.56.103. To manage and launch the virtual machines, we made use of VirtualBox, which can be found here (https://www.virtualbox.org/). 

#### Installing the depencies

##### General depencies
Make sure you have python3 installed.
You might need to install the dependencies listed in the requirements.txt -folder.
If you want to evaluate the project using the Evaluation branch, psutil is an additional requirement. This can be installed using pip3 install psutil.
If you have trouble with any of these Python commands, you might need to activate the Virtual environment (venv) for the project in question.
The venv is activated at the root of the node folder (for example, in the 'client' -folder) by running:

     . bin/activate
     
After activating the venv, when you are done with running the apps, you can deactivate it by simply running:

     deactivate
     
To install the dependencies in a client, run:
     
     pip3 install -r requirements.txt
     
##### Installing Kafka
In our set-up Kafka is expected to be run as a service on the Kafka node. There are two scripts you can use on this repository, one for launching the Kafka and included Zookeeper service, and one for stopping it, more information can be found in the How to run the Components section. It is assumed that Kafka has one broker available for use. There are multiple guides available for installing Kafka online, here is an example guide for installing Kafka as a service on Ubuntu environments (https://tecadmin.net/how-to-install-apache-kafka-on-ubuntu-20-04/).

### Using Docker (alternative)

For some duration of the project we also used Docker for development. However, we cannot guarantee all of the functionalities when using Docker and recommend users to use the virtual machine environment.

To run the project with Docker there is a docker-compose.yml -file located in the "kafka" -folder of the repository.
In the folder run:

     docker-compose up -d

That should start all the containers connected to a default Docker network so that they are visible to each other.

If the docker-compose.yml fails to start the containers, make sure you have local images of each container, and that they are named as expected by the docker-compose.yml

#### Creating local docker images images

Go to each components folder, where the corresponding Dockerfile is located and run:

     docker build -t <desired name for image> .
     
#### Running the game logic only with Docker

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


## How to run the components

### Client

Use the flags given below as necessary depending on if you are running the load_balancer on the VM,
in Docker, or locally.

Go to the client -folder
run: 
     
     python3 client_main.py [ -vm | -docker ]

### Server

Use the flags given below as necessary depending on if you are running the load_balancer on the VM,
in Docker, or locally.

Go to the server -folder
run: 
    
     python3 server_main.py [ -vm | -docker ]

### Load balancer

The load balancer is assumed to run on the VM machine called Client with IP address 192.168.56.101. 

When running the load_balancer, the environment variable ENV can be used to determine the address and port of the Kafka instance. The options for the environment variables are vm, production, and docker. Below is each variable laid out and explained.

| Value | Use case |
| -------- | -------- |
| vm       | When running the Kafka instance in a VM |
| production | When running the Kafka instance in a VM |
| docker | When running the Kafka instance with the provided docker-compose.yml |

If you are running the Kafka instance on the local machine, you do not need to provide this environment variable.

To run the load balancer, first go to the load_balancer folder.
Then, run the following: 

     export FLASK_APP=load_balancer.py
     export ENV=<the value from the table above that suits the environment>
    
     flask run --host=0.0.0.0
The --host variable is the ip address the load balancer will be visible on. In the above example it is set to 0.0.0.0, but if there occur issues feel free to change it to what is suitable for the design of your own networking environment.

### Kafka and its kafka zookeeper

The kafka node is assumed to run on the VM called Server with IP address 192.168.56.103.
Only the load balancer is aware of the IP address of the kafka node, the game servers and clients need to request it from the load balancer.
To run the kafka and zookeeper services, please run the startKafka.sh script. To check if the kafka service is properly running, try the command sudo systemctl status kafka.
To stop the kafka and zookeeper service, please run the stopKafka.sh script.

See the kafka documentation (https://kafka-python.readthedocs.io/en/master/) for more information.


## Testing the project's support of multiple topic

A special topic "**balancer-special**" is defined. It can be a topic used by the load balancer and one server to communicate or shared by all servers to communicate with the load balancer. (I prefer the former solution) The script *server/temp_add_topic.py* is just a temporary script for testing.

To try out the new feature:

1. Run the *server_game.py* directly.
2. If you want, run two *client_game.py* and possibly play the game as usual.
3. Run the *temp_add_topic.py* and enter some unique topic names to add
4. Choose any one of the new topics you just created, and manually replace the fixed topic name at the bottom of *client_game.py* with it. Then run two *client_game.py*, and you can see the game run as desired. This means the system  supports adding new Kafka topics and using those topics to communicate between game server and game client.

