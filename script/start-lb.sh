#!/bin/bash
# This should be run as follows:
# $ source ./start-lb.sh

# Set the home directory of LogicBlox
LOGICBLOX_HOME=~/tools/logicblox-linux-4.20.1 

# This part should follow how to run LogicBlox
# https://developer.logicblox.com/download/
source $LOGICBLOX_HOME/etc/profile.d/logicblox.sh
source $LOGICBLOX_HOME/etc/bash_completion.d/logicblox.sh

lb services start