## MongoDB Project (CS5424 Distributed Databse)

### Prerequisite
1. Operating System Linux x86_64
2. Python 3.7.3
3. Pip 19.0.3
4. MongoDB 4.2 

### Environment Setup
1. Install pipenv: `pip install pipenv --user`
2. Install python libraries: `pipenv install -r requirements.txt`
3. Untar MongoDB 4.2: `tar -xzvf mongodb-linux-x86_64-rhel70-4.2.0.tgz`
4. Unpack downloaded package using tar -zxvf mongodb-linux-x86_64-rhel70-4.2.0.tgz.
5. Create directory /mongodb-linux and move mongodb-linux-x86_64-rhel70-4.2.0.tgz into the directory.
6. Within /mongodb-linux:
   
   * Create a directory /log/mongodb and place an empty file named, mongod.log.
   * Create a directory /lib/mongo.
   * Create files named mongod.conf and mongos.conf.
  
7. Add the following export lines into .bash_profile.
  
   ```
   export MONGODB_HOME=/temp/mongodb-linux/mongodb-linux-x86_64-rhel70-4.2.0
   export PATH=$MONGODB_HOME/bin:$PATH
   ```
   
8. To run mongo instance (for config and shard server) use the following command:
    ```
   mongod --config /temp/mongodb-linux/mongod.conf
   ```
9. To run mongos / mongo router use the following command:
   ```
   mongos --config /temp/mongodb-linux/mongos.conf
   ```
10. To check shards status on mongos shell, run:
   ```
   sh.status()
   ```
11. To check config replica servers on mongod shell (config), run:
   ```
   rs.status()
   ```
#### Create Config Server Replica Set
1. Overwrite mongod.conf with the following:
    ````
    # mongod.conf
    
    # where to write logging data.
    systemLog:
      destination: file
      logAppend: true
      path: /temp/mongodb-linux/log/mongodb/mongod.log
    
    # Where and how to store data.
    storage:
      dbPath: /temp/mongodb-linux/lib/mongo
      journal:
        enabled: true
    
    # how the process runs
    processManagement:
      fork: true  # fork and run in background
    
    # network interfaces
    net:
      port: 27019
      bindIp: <ip addr of this server>
    
    replication:
      replSetName: cluster1
    
    sharding:
      clusterRole: configsvr
    ````
2. Start mongod instance using
`mongod --config /temp/mongodb-linux/mongod.conf`

3. Repeat in all 3 config servers.

4. Connect a mongo shell to one of the config server members. 
`mongo --host <hostname> --port 27019`

5. Initiate the config servers by executing the following command:
    ````
    rs.initiate(
      {
        _id: "cluster1",
        configsvr: true,
        members: [
          { _id : 0, host : "<ip addr of config server 1>:27019" },
          { _id : 1, host : "<ip addr of config server 2>:27019" },
          { _id : 2, host : "<ip addr of config server 3>:27019" }
        ]
      }
    )
    ````
6. Set the primary config serve by executing the following command in one of the 3 replica set:
`rs.isMaster()`

#### Create Shard Replica Set
1. Overwrite mongod.conf with the following in all 3 shard servers:
    ````
    #   http://docs.mongodb.org/manual/reference/configuration-options/
    
    # where to write logging data.
    systemLog:
      destination: file
      logAppend: true
      path: /temp/mongodb-linux/log/mongodb/mongod.log
    
    # Where and how to store data.
    storage:
      dbPath: /temp/mongodb-linux/lib/mongo
      journal:
        enabled: true
    
    # how the process runs
    processManagement:
      fork: true  # fork and run in background
    
    # network interfaces
    net:
      port: 27018
      bindIp: <ip addr of this server>
    
    replication:
      replSetName: shard1
    
    sharding:
      clusterRole: shardsvr
    ````
2. Start mongod instance.
`mongod --config /temp/mongodb-linux/mongod.conf`

3. Repeat in all 3 shard servers.
    - To run 3 shard servers and 3 config servers in 5 nodes, 1 server has to run 2 mongod instances (both shard and config).
    - Because `mongod.conf` is used as the configuration file for the config server instance, the above another file name must be used.
    - Add a `mongodShard.conf` and use the following:
    ````
    #   http://docs.mongodb.org/manual/reference/configuration-options/
    
    # where to write logging data.
    systemLog:
      destination: file
      logAppend: true
      path: /temp/mongodb-linux/log/mongodb/mongod2.log
    
    # Where and how to store data.
    storage:
      dbPath: /temp/mongodb-linux/lib/mongoShard
      journal:
        enabled: true
    
    # how the process runs
    processManagement:
      fork: true  # fork and run in background
    
    # network interfaces
    net:
      port: 27018
      bindIp: <ip addr of this server>
    
    replication:
      replSetName: shard1
    
    sharding:
      clusterRole: shardsvr
    ````
   - Start mongod instance. `mongod --config /temp/mongodb-linux/mongodShard.conf`

4. Connect a mongo shell to one of the shard server 
`mongo --host <hostname> --port 27018`

5. Initiate the shard replica servers by executing the following command:
    ````
    rs.initiate(
       {
         _id : "shard1",
         members: [
           { _id : 0, host : "<ip addr of shard server 1>:27018" },
           { _id : 1, host : "<ip addr of shard server 2>:27018" },
           { _id : 2, host : "<ip addr of shard server 3>:27018" }
         ]
       }
     )
    ````

6. Shard server that initiates the shard replica servers will automatically becomes the primary shard server.
Connect a mongo shell to the mongos (To be completed after Mongo Router Setup).
`mongo --host <hostname> --port 27017`

7. Execute the following in mongos to add shard to cluster:
    ````
    sh.addShard( "shard1/<ip addr of shard server 1>:27018")
    sh.addShard( "shard1/<ip addr of shard server 2>:27018")
    sh.addShard( "shard1/<ip addr of shard server 3>:27018")
    ````

#### Mongo Router Setup
1. Overwrite mongos.conf with the following:
    ````
    net:
      port: 27017
      bindIp: <ip addr of this server>
    sharding:
      configDB: cluster1/<ip addr of config server 1>:27019, <ip addr of config server 2>:27019, <ip addr of config server 3>:27019
    ````
2. Start mongos instance.
`mongos --config /temp/mongodb-linux/mongos.conf`

3. Repeat in all 5 config servers.


###  Data Model Setup
Run `pipenv run python load_data_mongodb.py [IP] [Database Name]`

### Run Transactions/Experiments
Run `pipenv run python xact_parser_mongodb.py [IP] [Database Name]`

