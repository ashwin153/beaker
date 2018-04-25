#!/bin/bash
####################################################################################################
#                                    Install Pants Dependencies                                    #
#              https://github.com/pantsbuild/pants/blob/master/README.md#requirements              #
####################################################################################################
sudo apt-get update && sudo apt-get -y install curl build-essential python python-dev openjdk-8-jdk

####################################################################################################
#                                         Bootstrap Client                                         #
#                  https://github.com/ashwin153/beaker/tree/master/beaker-client                   #
####################################################################################################
./pants -q run beaker-client/src/main/scala:bin -- "$@"