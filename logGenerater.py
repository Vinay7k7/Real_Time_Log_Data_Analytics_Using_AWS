import random
from random import choice
import time
import faker
from datetime import datetime
import boto3

# Initialize Faker to generate fake data
fak = faker.Faker()

# Function to generate random log data
def generate_log_data():
    return '%s - - [%s] "%s %s HTTP/1.0" %s %s "%s" "%s" %s' % (
        fak.ipv4(),
        fak.date_time_this_decade(),
        choice(['GET', 'POST', 'PUT', 'DELETE']),
        choice(['/usr', '/usr/admin', '/usr/admin/developer', '/usr/login', '/usr/register']),
        choice(['303', '404', '500', '403', '502', '304', '200']),
        str(int(random.gauss(5000, 50))),
        choice(['-', fak.uri()]),
        choice(['Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0',
                'Mozilla/5.0 (Android 10; Mobile; rv:84.0) Gecko/84.0 Firefox/84.0',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
                'Mozilla/5.0 (Linux; Android 10; ONEPLUS A6000) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Mobile Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4380.0 Safari/537.36 Edg/89.0.759.0',
                'Mozilla/5.0 (Linux; Android 10; ONEPLUS A6000) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.116 Mobile Safari/537.36 EdgA/45.12.4.5121',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36 OPR/73.0.3856.329',
                'Mozilla/5.0 (Linux; Android 10; ONEPLUS A6000) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36 OPR/61.2.3076.56749',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
                'Mozilla/5.0 (iPhone; CPU iPhone OS 12_4_9 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.2 Mobile/15E148 Safari/604.1']),
        random.randint(1, 5000)
    )

# Initialize the Kinesis Firehose client
firehose_client = boto3.client('firehose', region_name='your_aws_region')  # Replace 'your_aws_region' with your AWS region

# Main loop to generate and send log data
while True:
    log_data = generate_log_data()
    print(log_data)  # Print log data to console
    firehose_client.put_record(
        DeliveryStreamName='your_firehose_delivery_stream',  # Replace 'your_firehose_delivery_stream' with your Firehose delivery stream name
        Record={'Data': log_data + '\n'}
    )
    time.sleep(random.randint(1, 7))  
