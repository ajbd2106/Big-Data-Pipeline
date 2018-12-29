Data Source to Kafka
====================

Transfers data from a CSV file to a Kafka topic.  
Before sending data to Kafka, each CSV line is cleaned using user defined functions.  


Using Kafka Producer
--------------------

Place a CSV file into "/src/kafka_producer".  
Modify the settings and the user defined cleaner functions.  
Run the Kafka Producer with:  

::

  python src/kafka_producer/kafka_csv_producer.py