FROM ubuntu:latest
MAINTAINER Ashwin Madavan (ashwin.madavan@gmail.com)

####################################################################################################
#                                    Install Pants Dependencies                                    #
#              https://github.com/pantsbuild/pants/blob/master/README.md#requirements              #
####################################################################################################
RUN apt-get update && apt-get -y install curl build-essential python python-dev openjdk-8-jdk

####################################################################################################
#                                         Compile Beaker                                           #
#                    Automatically bootstraps Pants and downloads dependencies.                    #
####################################################################################################
COPY . /beaker/
RUN cd /beaker && ./pants compile ::
WORKDIR /beaker