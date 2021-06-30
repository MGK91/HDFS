import os
import sys
import subprocess
f6 = open("test6.txt", "w")
command5 = "/opt/hadoop/hadoop-3.3.0/bin/hdfs dfs -ls -R |sort -k6,7 |awk '$1 !~ /^d/' |tr -s \" \" | cut -d' ' -f6-8 | grep \"^[0-9]\"|awk 'BEGIN{ MIN=60; LAST=60*MIN; \"date +%s\" | getline NOW } { cmd=\"date -d'\\''\"$1\" \"$2\"'\\'' +%s\"; cmd | getline WHEN; DIFF=NOW-WHEN; if(DIFF < LAST){ print $3 }}'|awk NF|tee HDFS_LAST_UPDATED_FILE.txt"
#command5 = "/opt/hadoop/hadoop-3.3.0/bin/hdfs dfs -ls | tr -s \" \" | cut -d' ' -f6-8 | grep \"^[0-9]\" | awk 'BEGIN{ MIN=1; LAST=60*MIN; \"date +%s\" | getline NOW } { cmd=\"date -d'\\''\"$1\" \"$2\"'\\'' +%s\"; cmd | getline WHEN; DIFF=NOW-WHEN; if(DIFF > LAST){ print $3 }}'|tee HDFS_DIR.txt"
value5 = subprocess.call(command5, shell=True, stdout=f6)
#f7 = open("test7.txt", "w")
#command6 = "for i in `cat HDFS_DIR.txt`;do /opt/hadoop/hadoop-3.3.0/bin/hdfs dfs -ls $i/*|awk '{print $8}';done"
#value7 = subprocess.call(command6, shell=True, stdout=f7)
f3 = open("test3.txt", "w")

command = "for i in `cat test6.txt`;do /opt/hadoop/hadoop-3.3.0/bin/hdfs fsck -blocks -files $i|awk 'flag{ if (/Status/){printf \"%s\", buf; flag=0; buf=\"\"} else buf = buf $0 ORS}; /FSCK/{flag=1}'|awk NF |tee  HDFS_BLOCK.txt;done"
value2 = subprocess.call(command, shell=True, stdout=f3)
print("Block details of file that got generated in last one hour")
block_file_read = open("test3.txt", "r")
BLOCK_FILE = block_file_read.read()
print(BLOCK_FILE)
#subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE).stdout.read()
command1 = "for i in `cat test6.txt`;do /opt/hadoop/hadoop-3.3.0/bin/hdfs fsck -locations -files $i|awk 'flag{ if (/Status/) {printf \"%s\",buf; flag=0; buf=\"\"} else buf = buf $0 ORS};/FSCK/{flag=1}'|awk NF|tee HDFS_LOC.txt;done"
command2 = "for i in `cat test6.txt`;do /opt/hadoop/hadoop-3.3.0/bin/hdfs fsck -racks -blocks -locations -files $i|awk 'flag{ if (/Status/){printf \"%s\", buf; flag=0; buf=\"\"} else buf = buf $0 ORS}; /FSCK/{flag=1}' | awk -F'[' '{print $2}' |awk NF| tee HDFS_RACK.txt;done"
#command2 = "cat %s" %(command1)
#command3 = "%s |
##output = os.popen(command1)
##output_srtd = output.read()
##print(output_srtd)
f = open("test.txt", "w")
value = subprocess.call(command1,  shell=True, stdout=f)
print("Location details of Last Generated file")
location_file_read = open("test.txt", "r")
LOCATION_FILE = location_file_read.read()
print(LOCATION_FILE)
#print(value)
#f = open("HDFS_LOC.txt", "r")
#print(f.readlines())
print("Rack and Data node details of Last Generated file")
rack_file_read = open("HDFS_RACK.txt", "r")
RACK_FILE = rack_file_read.read()
print(RACK_FILE)
f1 = open("test1.txt", "w")
value1 = subprocess.call(command2, shell=True, stdout=f1)
print("Newest file in the test directory ")
f4 = open("test4.txt", "w")
command3 = "/opt/hadoop/hadoop-3.3.0/bin/hdfs dfs -ls -R test/|sort -r -k6,7|head -1"
value5 = subprocess.call(command3, shell=True, stdout=f4)
old_file_read = open("test4.txt", "r")
NEW_FILE = old_file_read.read()
print(NEW_FILE)
print("Oldest file in test directory")
f5 = open("test5.txt", "w")
command4 = "/opt/hadoop/hadoop-3.3.0/bin/hdfs dfs -ls -R test/|sort -r -k6,7|tail -1"
value4 = subprocess.call(command4, shell=True, stdout=f5)
#print(f5.readlines())
newfile_read = open("test5.txt", "r")
OLD_FILE = newfile_read.read()
print(OLD_FILE)
#print(value1)
#f1 = open("HDFS_BLOCK.txt", "r")
#print(f1.readlines())
