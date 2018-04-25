#!/bin/bash
####################################################################################################
#                                         Bootstrap Client                                         #
#                  https://github.com/ashwin153/beaker/tree/master/beaker-client                   #
####################################################################################################
./pants -q run beaker-client/src/main/scala:bin -- "$@"