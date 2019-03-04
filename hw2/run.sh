#!/usr/bin/env bash

CLASSES=/s/bach/e/under/millardk/cs455/hw2/build/classes/java/main
SCRIPT="cd $CLASSES;
java -cp . cs455.scaling.client.Client ${1} ${2} ${3}"

#$1 is the server hostname
#$2 is the server port
#$3 is the message-rate
#$4 is the clients per machine


for ((j=0; j<${4}; j++))
do
	COMMAND='gnome-terminal'
	for i in `cat machine_list`
	do
		echo 'logging into '$i
		OPTION='--tab -e "ssh -t '$i' '$SCRIPT'"'
		COMMAND+=" $OPTION"
		done
		eval $COMMAND &
	done
