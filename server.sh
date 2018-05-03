#!/bin/bash
####################################################################################################
#                                        Server Configuration                                      #
#  https://github.com/ashwin153/beaker/blob/master/beaker-server/src/main/resources/reference.conf #
####################################################################################################
PORT=9090
OPTS=""

while getopts ":hc:l:o:p:" opt; do
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
    l ) OPTS+=" --jvm-run-jvm-options="\""-Dlog4j.configuration=file:${OPTARG}"\"
        ;;
    o ) OPTS+=" --jvm-run-jvm-options="\""-D${OPTARG}"\"
        ;;
    p ) PORT=${OPTARG}
        ;;
  esac
done

HOST=$(hostname -I | cut -d' ' -f1)
OPTS+=" --jvm-run-jvm-options="\""-Dbeaker.server.address=${HOST}:${PORT}"\"

####################################################################################################
#                                        JVM Configuration                                         #
#  https://github.com/ashwin153/beaker/blob/master/beaker-server/src/main/resources/reference.conf #
####################################################################################################
FREE=$( free -m | awk  'FNR == 2 {print $4}')
LOG2=$( echo "l($FREE)/l(2)" | bc -l )
MAX_HEAP=$(( 1 << ${LOG2%.*} ))
MIN_HEAP=$(( 1 << ${LOG2%.*} - 1 ))
OPTS+=" --jvm-run-jvm-options="\""-Xms${MIN_HEAP}M -Xmx${MAX_HEAP}M"\"

####################################################################################################
#                                        Bootstrap Server                                          #
#                  https://github.com/ashwin153/beaker/tree/master/beaker-server                   #
####################################################################################################
eval ./pants run beaker-server/src/main/scala:bin ${OPTS}