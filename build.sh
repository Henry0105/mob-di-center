#!/usr/bin/env bash
set -x -e

if [ $# -eq 0 ]; then
    echo "build.sh 215|26"
    exit -1
fi

mobdi_HOME="$(cd "`dirname "$0"`"; pwd)"
cd ${mobdi_HOME}
export JAVA_HOME=/home/dba/jdk1.8.0_45
export PATH=$JAVA_HOME/bin:$PATH

export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=256m"


# mvn clean scalastyle:check package -P$1
mvn clean  package -P$1

latest=$(readlink -f ${mobdi_HOME}/)
version=$(cat ${mobdi_HOME}/dist/conf/version.properties)
# version="v2.3.0"
home_version_control=/home/dba/version_control/${version}

email_report=/home/dba/mobdi_center/email_report
kpi_qc=/home/dba/mobdi_center/kpi_qc
mobdi=/home/dba/mobdi_center/mobdi
mobdi_sec=/home/dba/mobdi_center/mobdi_sec
sort_system=/home/dba/mobdi_center/sort_system
crawler=/home/dba/mobdi_center/crawler
conf=/home/dba/mobdi_center/conf

ln -snf ${mobdi_HOME}  ${home_version_control}
ln -snf ${home_version_control}/dist/sbin/email_report  ${email_report}
ln -snf ${home_version_control}/dist/sbin/kpi_qc  ${kpi_qc}
ln -snf ${home_version_control}/dist/sbin/mobdi  ${mobdi}
ln -snf ${home_version_control}/dist/sbin/mobdi_sec  ${mobdi_sec}
ln -snf ${home_version_control}/dist/sbin/sort_system  ${sort_system}
ln -snf ${home_version_control}/dist/sbin/crawler  ${crawler}
ln -snf ${home_version_control}/dist/sbin/conf  ${conf}

yes|cp -fr ${home_version_control}/dist/lib /home/dba/

# update_info=`git diff --stat  head~ head`

# java -jar ./lib/mailSender-1.0.0.jar "walle" "sucess" "zhoup@mob.com,zhtli@mob.com"
# 将所有脚本加上可执行权限
chmod +x `find -iname \*.sh `