#!/bin/bash
####################################################################################################
#                                        Parse Configuration                                       #
#  https://github.com/ashwin153/beaker/blob/master/beaker-server/src/main/resources/reference.conf #
####################################################################################################
host=$(hostname -I | cut -d' ' -f1)
port=9090
conf=""
pts=""

while getopts ":hc:dn:o:p:r:" opt; do
  case $opt in
    h ) echo "Usage: $0 [option...]                                         " >&2
        echo
        echo "   -h                         Displays this help message.     "
        echo
        echo "   -c                         Path to configuration file.     "
        echo "   -o                         Configuration overrides.        "
        echo "   -p                         Port number. (9090)             "
        echo
        exit 1
        ;;
    c ) conf="-v ${OPTARG}:/beaker/beaker-server/src/main/resources/application.conf"
        ;;
    o ) opts+="--jvm-run-jvm-options="\""-D${OPTARG}"\"" "
        ;;
    p ) port=${OPTARG}
        ;;
  esac
done

####################################################################################################
#                                         Bootstrap Server                                         #
#                               https://docs.docker.com/get-started/                               #
####################################################################################################
opts+="--jvm-run-jvm-options="\""-Dbeaker.server.address=${host}:${port}"\"
sudo apt-get update && sudo apt-get -y install curl build-essential python python-dev openjdk-8-jdk
eval ./pants run beaker-server/src/main/scala:bin ${opts}