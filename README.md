# Background
MicroMQ is a lightweight message queue system whose code lines are only about 3K. This project is aimed to help those programmer beginners learn how to build a complete system which may include storage, HTTP interface, configuration, background task, and so on.
It is base on Spring Boot, and uses MySQL as default storage. You can also extend it to use other storages like Redis.

# Install
It is a learning project, and not well tested. If you want to deploy it on production environment, please completely test it first. You can learn it, run it, modify it by using IDEA and maven. Just import it into IDEA and try it as your will.

# Usage
1. install mysql, and create necessary tables, SQL script file can be found under directory './src/main/resources/script/MicroMQ.sql'
2. use maven or IDEA to build the project
3. change the configuration under directory './src/main/resources/config'
4. run the project through IDEA, main class is: MQBootstrap
5. access 'http://localhost:8080' to try some operations.

# How to learn
1. knowledge about Spring Boot 
2. knowledge about MySQL
3. key classes: MQNode, MQOperation, MQClient, MQQueue; key interface: IPull, ISave, NodeGroupStrategy, IMessageDao, IReceiptDao
4. access 'http://localhost:8080' to try some operations, access mysql to check the data

# Extend
You can extend the following features:
1. storage: just implement IMessageDao and IReceiptDao, and then initiate storage at MQNode.build function.
2. group: just implement NodeGroupStrategy, and then initiate group strategy at MQNode.build function.
3. queue mode: implement IPull to add new kind of pull operation, implement ISave to add new kind of save mode for queue.
4. more functions: add more Spring Boot controllers to support new functions.

# Maintainers
Watson &lt;yunhua.lee@gmail.com&gt;

# License
Apache License 2.0
