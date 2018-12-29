from setuptools import setup, find_packages



with open("README.rst") as f:
  readme = f.read()

setup(
  name="Data-Source-To-Kafka",
  version="0.1.0",
  description="Transfer data from a data source (CSV file) to a Kafka topic.",
  long_description=readme,
  
  author="Mislav Jaksic",
  author_email="jaksicmislav@gmail.com",
  url="https://github.com/MislavJaksic/Big-Data-Pipeline/Data-Source-To-Kafka",
  
  packages=find_packages(exclude=("sample_data")),
  
  entry_points={"console_scripts" : ["project_name = src.kafka_producer.kafka_csv_producer:Run"]}
)
