#!/bin/bash
####################################################################################################
#                                        Parse Configuration                                       #
#  https://github.com/ashwin153/beaker/blob/master/beaker-server/src/main/resources/reference.conf #
####################################################################################################
name="default"
port=9090
conf=""
opts=""
sync=""
region="us-west-2"

while getopts ":hc:dn:o:p:r:" opt; do
  case $opt in
    h ) echo "Usage: $0 [option...]                                         " >&2
        echo
        echo "   -h                         Displays this help message.     "
        echo
        echo "   -c                         Path to configuration file.     "
        echo "   -d                         Daemonize container. (false)    "
        echo "   -n                         Instance name. (default)        "
        echo "   -o                         Configuration overrides.        "
        echo "   -p                         Port number. (9090)             "
        echo "   -r                         AWS region. (us-west-2)         "
        echo
        exit 1
        ;;
    p ) port=${OPTARG}
        ;;
    c ) conf="-v ${OPTARG}:/beaker/beaker-server/src/main/resources/application.conf"
        ;;
    o ) opts+="-D${OPTARG},"
        ;;
    d ) sync="-d"
        ;;
    n ) name="${OPTARG}"
        ;;
    r ) region="${OPTARG}"
        ;;
  esac
done

####################################################################################################
#                                     Bootstrap Docker Container                                   #
#                               https://docs.docker.com/get-started/                               #
####################################################################################################
docker-machine create \
  --driver amazonec2 \
  --amazonec2-open-port ${port} \
  --amazonec2-region us-west-2 ${name}

eval $(docker-machine env ${name})
host=$(docker-machine ip ${name})
opts+="-Dbeaker.server.address=${host}:${port}"

docker run ${sync} \
  --net="host" \
  -p ${port}:${port} \
  ${conf} \
  ashwin153/beaker \
  ./pants run beaker-server/src/main/scala:bin \
  --jvm-run-jvm-options="${opts}" || \
echo "\e[31mUnable to create a docker container.\e[39m" && \
exit 1
