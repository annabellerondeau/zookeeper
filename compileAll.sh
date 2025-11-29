#!/bin/bash

# compile task
echo "Compiling TASK..."
cd task && ./compiletask.sh
if [ $? -ne 0 ]; then
    echo "ERROR: Task compilation failed."
    exit 1
fi

# compile dist
echo "Compiling DIST server..."
cd ../dist && ./compilesrvr.sh
if [ $? -ne 0 ]; then
    echo "ERROR: Server compilation failed."
    exit 1
fi

# compile clnt
echo "Compiling CLNT client..."
cd ../clnt && ./compileclnt.sh
if [ $? -ne 0 ]; then
    echo "ERROR: Client compilation failed."
    exit 1
fi

echo "Build finished"
# Return to the root directory
cd ..