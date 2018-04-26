#!/bin/bash
####################################################################################################
#                                         Bootstrap Server                                         #
#  https://github.com/ashwin153/beaker/blob/master/beaker-server/src/main/resources/reference.conf #
####################################################################################################
host=$(hostname -I | cut -d' ' -f1)
port=9090
opts=""

while getopts ":hc:dn:o:p:r:" opt; do
  case $opt in
    h ) echo "Usage: $0 [option...]                                         " >&2
        echo
        echo "   -h                         Displays this help message.     "
        echo
        echo "   -c                         Path to configuration file.     "
        echo "   -l                         Path to log4j properties file.  "
        echo "   -o                         Configuration overrides.        "
        echo "   -p                         Port number. (9090)             "
        echo
        exit 1
        ;;
    c ) eval cp ${OPTARG} /beaker-server/src/main/resources/application.conf
        trap 'rm /beaker-server/src/main/resources/application.conf' EXIT
        ;;
    l ) opts+="--jvm-run-jvm-options="\""-Dlog4j.configuration=file:${OPTARG}"\"" "
        ;;
    o ) opts+="--jvm-run-jvm-options="\""-D${OPTARG}"\"" "
        ;;
    p ) port=${OPTARG}
        ;;
  esac
done

opts+="--jvm-run-jvm-options="\""-Dbeaker.server.address=${host}:${port}"\"
eval ./pants run beaker-server/src/main/scala:bin ${opts}