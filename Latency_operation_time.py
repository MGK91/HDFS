import pandas as pd
import json
import numpy as np
with open('/tmp/test.json', 'r') as f:
    data = json.load(f)
    #print("Type:", type(data))
    #print("\nTimestamp:", data['timestamp'])
    #print("\nWindow:", data['windows'])
    #items = len(dict2)
total = 0
value_dict = {}
value_dict1 = {}
print(data)
for window in data['windows']:
    windows_length = len(window)
    for opstype in window['ops']:
        ops_length = len(opstype)
        for opstype1 in opstype['opType'].split():
            #print(opstype1)
            print("Ops type of ", opstype1)
            #operation[opstype1] = opstype1
            #value_dict.append(opstype1, window.get('windowLenMs'))
            #value_dict[opstype1] = window.get('windowLenMs')
            window_value = window.get('windowLenMs')
            print(window.get('windowLenMs'))
            total = total+window_value
            #value_dict[opstype1] = window.get('windowLenMs')
            #value_dict[opstype1] = window_value
            #value_dict[opstype1]
            value_dict[opstype1] = window.get('windowLenMs')
     #value_dict1 = {k:v for k,v in value_dict.items()}
    #print(value_dict)
#for i in range(windows_length):
#    for j in range(ops_length):
#        value_dict[j] = value_dict[i]
#value_dict1 = value_dict
#print(total)
#print(value_dict)
print("####################################################")
print("Printing the operation that took more time")
print("------------------------------------------")
max_key = max(value_dict, key=value_dict.get)
print(max_key)
print("####################################################")
print("90th percentile for each oepration")
print("-----------------------------------")
res = {k: np.percentile(v, 90) for k,v in value_dict.items()}
print(res)
print("#####################################################")
print("95th percentile for each oepration")
print("----------------------------------")
res1 = {k: np.percentile(v, 95) for k, v in value_dict.items()}
print(res1)
#for k, v in value_dict.items():
#    print("Percentile of Latency for operation", k)
#    np.percentile(

#print(data)
#df = pd.read_json('/tmp/test.json')
#print(df.head(10))
#print(df.shape)
#print(df.columns)
#df1 = df["windows"].groupby(level=0).apply(lambda x: x.tolist()).to_dict()
#df1 = df["windows"].groupby(level=0).apply(lambda x: x.tolist()).to_dict()
#print(df1.columns)
#{k: v for k, v in sorted(df1.items(), key=lambda item: item[1])}

#df1 = df1["ops"].groupby(levle-0)
#for key, value in df1.items:
#    print(key)
#f = open('/tmp/test.json', 'r')
#data = json.load(f)
#for i in data['windows']:
#    if 'windowLenMs' in i:
#        print(i[1])
#f.close()