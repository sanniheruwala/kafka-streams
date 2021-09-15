from __future__ import print_function
import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions
from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL

kafka_bootstrap = 'localhost:9093'
kafka_topic = "internal_test_api"

cards = ReadFromMySQL(
    query="SELECT * FROM lookup.cards limit 10",
    host="localhost",
    database="lookup",
    user="root",
    password="password",
    port=3306,
    splitter=splitters.NoSplitter()  # you can select how to split query for performance
)

pipeline = beam.Pipeline(options=PipelineOptions())
data = pipeline| 'Read from Kafka' >> ReadFromKafka(consumer_config={'bootstrap.servers': kafka_bootstrap,
                                                           'auto.offset.reset': 'latest'},
                                          topics=['internal_test_api'])



