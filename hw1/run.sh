#!/bin/bash
test_home=~/cs455/hw1
for i in 'cat machine_list';
do
    echo 'logging into '${i}
    dbus-launch gnome-terminal -x bash -c "ssh -t ${i};'cd ${test_home}/build/classes/java/main; java cs455.overlay.node.MessagingNode maserati 10000;bash;'" &
done