#!/bin/bash
####################################################################################################
#                                        Parse Configuration                                       #
#  https://github.com/ashwin153/beaker/blob/master/beaker-server/src/main/resources/reference.conf #
####################################################################################################
eval $(docker-machine env)
host=$(docker-machine ip)
port=9090
conf=""
opts="-Dbeaker.server.address=${host}:${port}"

while getopts ":hp:c:o:" opt; do
  case $opt in
    h ) echo "Usage: $0 [option...]                                         " >&2
        echo
        echo "   -h                         Displays this help message.     "
        echo "   -p                         Port number. [0, 65535]         "
        echo "   -c                         Path to configuration file.     "
        echo "   -o                         Configuration overrides.        "
        echo
        exit 1
        ;;
    p ) port=${OPTARG}
        ;;
    c ) conf="-v ${OPTARG}:/beaker/beaker-server/src/main/resources/application.conf"
        ;;
    o ) opts+=",-D${OPTARG}"
        ;;
  esac
done

####################################################################################################
#                                     Bootstrap Docker Container                                   #
#                               https://docs.docker.com/get-started/                               #
####################################################################################################
docker run -d \
  -p ${port}:${port} \
  ${conf} \
  ashwin153/beaker \
  ./pants run beaker-server/src/main/scala:bin \
  --jvm-run-jvm-options="${opts}" || \
echo "\e[31mUnable to create a docker container.\e[39m" && \
exit 1
