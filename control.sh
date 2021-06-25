#!/bin/bash
APP="build/libs/FullNode"
JAR_NAME="$APP.jar"
MAX_STOP_TIME=60

checkpid() {
 pid=`ps -ef | grep $JAR_NAME |grep -v grep | awk '{print $2}'`
 return $pid
}

stopService() {
  count=1
  while [ $count -le $MAX_STOP_TIME ]; do
    checkpid
    if [ $pid ]; then
       kill -15 $pid
       sleep 1
    else
       echo "java-tron stop"
       return
    fi
    count=$[$count+1]
    if [ $count -eq $MAX_STOP_TIME ]; then
      kill -9 $pid
      sleep 1
    fi
  done
}


startService() {
 echo `date` >> start.log
 total=`cat /proc/meminfo  |grep MemTotal |awk -F ' ' '{print $2}'`
 xmx=`echo "$total/1024/1024*0.8" | bc |awk -F. '{print $1"g"}'`
 logtime=`date +%Y-%m-%d_%H-%M-%S`
 nohup java -Xmx$xmx -XX:+UseConcMarkSweepGC -XX:+PrintGCDetails -Xloggc:./gc.log\
 -XX:+PrintGCDateStamps -XX:+CMSParallelRemarkEnabled -XX:ReservedCodeCacheSize=256m\
 -XX:+CMSScavengeBeforeRemark -jar $JAR_NAME -c config.conf  >> start.log 2>&1 &

 pid=`ps -ef |grep $JAR_NAME |grep -v grep |awk '{print $2}'`
 echo "start java-tron with pid $pid on $HOSTNAME"
}


rebuild() {
  git pull
  ./gradlew build -x test
}

if [[ "$1" = "start" ]]; then
    startService
elif [[ "$1" = "stop" ]]; then
    stopService
elif [[ "$1" = "rebuild" ]]; then
    rebuild
elif [[ "$1" = "reload" ]]; then
    stopService
    startService
fi
