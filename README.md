## Big Data Pipeline

The pipeline is made out of multiple pipe segments which can, but don't have to, be connected to each other.  

### Data Source to Kafka Pipe Segment

An Apache Kafka Producer written in Python.  

* It transports data from a CSV file to a Kafka topic.  

For more information, take a look at the project's README.  

### Hive Redis To Spark To Hive

A pipe segment written in Java, constructed as a Maven project.  

1) It takes data from both Apache Hive and Redis  
2) It combines both into a single data object using a user written function  
3) It sends data objects into Apache Spark for analysis  
4) Finally, it stores the results into Apache Hive  

For more information, take a look at the project's Javadocs and source code.  

### Kafka To Hive

An Apache Kafka Consumer written in Java, constructed as a Maven project.  

* It transports data from a Kafka topic to a table in Apache Hive.  

For more information, take a look at the project's Javadocs and source code.  

### Tutorial Analysis and Theoretical Foundation

Apache Kafka, Apache Spark, Apache Hive and Redis were researched to see if they were the right tools for the job.  

If you are interested in the results of the analysis, follow the [GitHub link](https://github.com/MislavJaksic/KnowledgeRepository/tree/master/BigData).  