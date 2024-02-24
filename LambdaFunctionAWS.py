import json

def lambda_handler(event, context):
    records = event['records']
    for record in records:
        # Decode the base64 encoded data
        payload = base64.b64decode(record['data'])
        
        # Transform the log data into JSON format
        json_data = transform_to_json(payload)
        
        # Encode the JSON data back to base64
        record['data'] = base64.b64encode(json.dumps(json_data).encode('utf-8')).decode('utf-8')
    
    return {'records': records}

def transform_to_json(log_data):
    # Split the log data into individual fields
    fields = log_data.decode('utf-8').split(' ')
    
    # Extract relevant fields
    ip_address = fields[0]
    timestamp = fields[3][1:] + ' ' + fields[4][:-1]  # Concatenate date and time parts of the timestamp
    request_method = fields[5][1:]
    endpoint = fields[6]
    http_version = fields[7]
    status_code = fields[8]
    user_agent = ' '.join(fields[11:-2])[1:-1]  # Combine user agent fields into one
    
    # Create a dictionary representing the JSON structure
    json_data = {
        'ip_address': ip_address,
        'timestamp': timestamp,
        'request_method': request_method,
        'endpoint': endpoint,
        'http_version': http_version,
        'status_code': status_code,
        'user_agent': user_agent
    }
    
    return json_data
