import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

#Add "src" to sys.path -> "src" contains the package "kafka_producer"
#From now on, you can do an import like "from kafka. ..."