#!/bin/bash

# Prep work, adding config file
mkdir -p ~/.lsst
cd /opt/lsst/software/server
cp ./db-auth.yaml ~/.lsst/

# Assigning correct permissions and group
cd ~/
chmod -R 600 ./.lsst
chown -R lsst:lsst ./.lsst

# Changing to the user the LSST stack expects to operate with
su lsst

# Return to server dir for env setup
cd /opt/lsst/software/server

# Pull path bins
source /opt/lsst/software/stack/loadLSST.bash

# Install daf_butler
pip3 install git+https://github.com/lsst/daf_butler@master#egg=daf_butler

# Retrieve artifacts
mkdir -p /tmp/data
butler --long-log --log-level DEBUG retrieve-artifacts --collections=$2 s3://butler-config /tmp/data
