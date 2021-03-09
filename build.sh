#!/usr/bin/env bash
set -x -e

if [ $# -eq 0 ]; then
    echo "build.sh 215|26"
    exit 1
fi

mobdi_HOME="$(cd "`dirname "$0"`"; pwd)"
cd ${mobdi_HOME}
export JAVA_HOME=/home/dba/jdk1.8.0_45
export PATH=$JAVA_HOME/bin:$PATH

export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=256m"


mvn clean package -Pprod -Pspark2.4.3 -Dmaven.test.skip=true

MOBDI_HOME=/data/walle/mobdi_center
version=$(cat ${MOBDI_HOME}/dist/conf/version.properties)
mkdir -p /home/dba/mobdi_center_versions

ln -snf ${MOBDI_HOME} /home/dba/mobdi_center_versions/${version}

ln -snf /home/dba/mobdi_center_versions/${version}/dist/sbin/mobdi /home/dba/mobdi_center


yes|cp -fr /home/dba/mobdi_center_versions/${version}/dist/lib /home/dba/mobdi_center/lib

# update_info=`git diff --stat  head~ head`


# 将所有脚本加上可执行权限
chmod +x `find -iname \*.sh `





