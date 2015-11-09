Simple Distributed Filesystem:

This project implements a simple distributed file system with a Master-Slave design.
The master is elected through the bully protocol and failures are detected by gossip based membership communication.

How to compile: $ cd distFS $ mvn package
This will create a fat jar in the target folder. Copy the jar to all the node.
You can use the below command to copy the jar to all the nodes. 

$ for NUM in seq 1 1 7; do scp distFSFinal.jar fa15-cs425-g01-0$NUM:~; done

The setup expects one of the nodes to be an introducer. The rest of the nodes are just participants. A config xml defines the intoducer node and port to connect. The xml also has a bunch of other configuraion parameters. The same config xml should be given to all the nodes in the system.
To run the executable run the below command. 

//CHANGE THIS 
$ java -cp ~/distMemFinal.jar edu.uiuc.cs425.App Arg 1: path to the configuration xml