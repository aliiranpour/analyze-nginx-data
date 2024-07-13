import re
import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import numpy as np

# Step 1: Read and parse data

log_pattern = re.compile(
    r'(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<timestamp>[^\]]+)\] "(?P<method>\S+) (?P<url>\S+)(?: HTTP/\S+)" (?P<status>\d+) (?P<size>\d+)'
)

def parse_log_entry(entry):
    match = log_pattern.match(entry)
    if match:
        return match.groupdict()
    else:
        return None


def parse_log_file(file_path):
    with open(file_path, 'r') as file:
        log_data = [parse_log_entry(line) for line in file]
        

    # Step 2: Clean parsed data
    log_data = [entry for entry in log_data if entry is not None]
    return pd.DataFrame(log_data)



log_df = parse_log_file('nginx_logs - Copy.txt')

log_df['timestamp'] = pd.to_datetime(log_df['timestamp'], format='%d/%b/%Y:%H:%M:%S %z')
log_df['status'] = log_df['status'].astype(int)
log_df['size'] = log_df['size'].astype(int)
log_df['query_params'] = log_df['url'].str.extract(r'\?(.*)')


print(log_df.tail())

# step 3: Store data in mysql

try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',  
        password='',  
        database='Nginx_logs' 
    )
    cursor = conn.cursor()
    print("Connection successful!")

    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS nginx_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ip VARCHAR(15),
            timestamp DATETIME,
            method VARCHAR(10),
            url TEXT,
            status INT,
            size INT,
            query_params TEXT
        )
    ''')
    conn.commit()

    insert_query = '''
        INSERT INTO nginx_logs (ip, timestamp, method, url, status, size, query_params)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''
    for index, row in log_df.iterrows():
        cursor.execute(insert_query, (
            row['ip'], 
            row['timestamp'].to_pydatetime(),  
            row['method'], 
            row['url'], 
            row['status'], 
            row['size'], 
            row['query_params']
        ))
    conn.commit()

    cursor.close()
    conn.close()

except mysql.connector.Error as e:
    print("Error in connection:", e)

# step 4: Generate Visualizations


status_counts = log_df['status'].value_counts()
status_counts.plot(kind='bar')
plt.title('Requests per Status Code')
plt.xlabel('Status Code')
plt.ylabel('Count')
plt.show()

log_df.set_index('timestamp', inplace=True)
log_df['method'].resample('h').count().plot()
plt.title('Requests Over Time')
plt.xlabel('Time')
plt.ylabel('Request Count')
plt.show()

url_counts = log_df['url'].value_counts().head(10)
url_counts.plot(kind='bar')
plt.title('Top Requested URLs')
plt.xlabel('URL')
plt.ylabel('Count')
plt.show()


ip_counts = log_df['ip'].value_counts().head(10)

ip_counts.plot(kind='bar')
plt.title('Top 10 IPs by Request Count')
plt.xlabel('IP Address')
plt.ylabel('Count')
plt.show()


log_df['size'].describe()

plt.figure(figsize=(10, 6))
log_df['size'].plot(kind='hist', bins=50, edgecolor='black')
plt.title('Distribution of Response Sizes')
plt.xlabel('Response Size (bytes)')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()


