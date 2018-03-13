#!/bin/bash
####################################################################################################
#                               Declare Build Targets and Artifacts                                #
#                        https://github.com/ashwin153/beaker/wiki/Release                          #
####################################################################################################
targets=(
  "beaker-cluster/src/main/scala"
  "beaker-core/src/main/scala"
  "beaker-core/src/main/thrift"
)

artifacts=(
  "beaker-cluster_2.12"
  "beaker-core_2.12"
  "beaker-thrift"
)

####################################################################################################
#                                Publish Artifacts to Maven Central                                #
#                                          DO NOT MODIFY                                           #
####################################################################################################
branch=$(git symbolic-ref --short HEAD)
if [ -n "$(git status --porcelain)" ]; then 
  echo -e "Current branch \033[0;33m$branch\033[0m has uncommitted changes."
  exit 1
else
  read -p "Artifact version (defaults to incrementing patch version): " version
  read -r -p "$(echo -e -n "Confirm release of \033[0;33m$branch\033[0;0m? [y|N] ")" response
fi

if [[ "$response" =~ ^([yY][eE][sS]|[yY])+$ ]] ; then
  if [ -z "$version" ] ; then
    publish="./pants publish.jar --publish-jar-no-dryrun ${targets[@]}"
  else
    overrides=("${artifacts[@]/#/--publish-jar-override=com.madavan#}")
    overrides=("${overrides[@]/%/=$version}")
    publish="./pants publish.jar --publish-jar-no-dryrun ${overrides[@]} ${targets[@]}"
  fi

  if eval $publish ; then 
    /usr/bin/open -a "/Applications/Google Chrome.app" \
      'http://www.pantsbuild.org/release_jvm.html#promoting-to-maven-central'
    /usr/bin/open -a "/Applications/Google Chrome.app" \
      'https://oss.sonatype.org/#stagingRepositories'
  fi
fi
