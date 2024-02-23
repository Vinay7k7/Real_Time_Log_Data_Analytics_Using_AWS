import boto3
import json
import time
import random
from datetime import datetime

def read_logs_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        lines = response['Body'].iter_lines()
        return lines
    except Exception as e:
        print("Error reading log file from S3:", e)
        return []

def transform_log_to_json(log_line):
    parts = log_line.split(' ')
    if len(parts) >= 12:
        ip_address = parts[0]
        # Generate current timestamp
        timestamp = datetime.now().isoformat()
        http_method = parts[5].replace('"', '')
        request_uri = parts[6]
        http_status_code = parts[8]
        size = parts[9]
        user_agent = parts[11].replace('"', '')

        # Extracting operating system from user agent
        os_start_index = log_line.find(user_agent) + len(user_agent) + 1  # Adding 1 for the space
        os_end_index = log_line.find('"', os_start_index)
        operating_system = log_line[os_start_index:os_end_index]

        log_data = {
            "Ip": ip_address,
            "Time_stamp": timestamp,
            "HTTP_Method": http_method,
            "Request_URL": request_uri,
            "HTTP_Status_Code": http_status_code,
            "Size": size,
            "User_Agent": user_agent,
            "Operating_System": operating_system
        }
        return log_data
    else:
        return None

def main():
    bucket_name = 'log-data-inpoint'
    file_key = 'Row_Log_Data/access.log'
    stream_name = 'code-to-kinesis'  # Specify your Kinesis stream name here

    lines = read_logs_from_s3(bucket_name, file_key)
    
    try:
        kinesis_client = boto3.client('kinesis', region_name='us-east-1')  # Specify your AWS region here
        for line in lines:
            log_line = line.decode('utf-8').strip()
            log_data = transform_log_to_json(log_line)
            if log_data:
                # Convert log data to JSON
                json_data = json.dumps(log_data)
                # Put record to Kinesis stream
                kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=json_data.encode('utf-8'),  # Encode JSON data as bytes
                    PartitionKey=str(datetime.now().timestamp())  # Provide a partition key
                )
                # Print the sent log to the console
                print(json_data)
            else:
                print("Error: Log line format invalid:", log_line)
            
            # Randomly choose the sleep interval between 1 and 7 seconds
            sleep_interval = random.randint(1, 7)
            time.sleep(sleep_interval)
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
