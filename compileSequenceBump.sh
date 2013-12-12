#!/bin/bash


javac -Xlint:unchecked -cp . SequenceBump.java

#// SE December 12, 2013
# Add more paths below here, comment out the ones you dont want, dont modify them.

java -cp .:/Users/shaun/dev/oracle11jdbc/ojdbc6.jar SequenceBump jdbc:oracle:thin:@//192.168.5.36:1521/orca [password] [password]
