from kafka import KafkaProducer
import csv
import time
from kafka.errors import KafkaTimeoutError


# Reading the CSV as Dict
reader = csv.DictReader(open("../data/CIDDS-001-external-week1.csv", 'r'))
dict_list = []
for line in reader:
    dict_list.append(line)  

#Initialize producer 
# producer = KafkaProducer()
producer = KafkaProducer(bootstrap_servers='localhost:9092')

try:
    for log in dict_list:
        producer.send('log', key=b'server', value=bytes(str(log), 'utf-8'))
        print('Sent:', log)
        time.sleep(1)
except KafkaTimeoutError:
    print("Timeout: not possible to send the data.")
finally:
    producer.close()
