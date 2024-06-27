#!/bin/bash
thread_exist_fn(){
ps -ef | grep "$1" | grep -v grep &> /dev/null
if [ $? -eq 0 ];then
  echo "存在 $1 程序"
else
  echo "启动 $1 程序"
  cd /Users/alpha/Documents/GitHub/QT
  `$2` &
fi
}

ps -ef | grep 'data/stock_news.py' | grep -v grep &> /dev/null
if [ $? -eq 0 ];then
  echo "存在data/stock_news.py程序"
else
  echo "启动data/stock_news.py程序"
  cd /Users/alpha/Documents/GitHub/QT
  nohup /Users/alpha/opt/anaconda3/envs/sklearn-env/bin/python data/stock_news.py > stock_news_log.txt 2>&1 &
fi



ps -ef | grep 'monitor/real_ai_new_monitor.py' | grep -v grep &> /dev/null
if [ $? -eq 0 ];then
  echo "存在monitor/real_ai_new_monitor.py程序"
else
  echo "启动monitor/real_ai_new_monitor.py程序"
  cd /Users/alpha/Documents/GitHub/QT
  nohup /Users/alpha/opt/anaconda3/envs/sklearn-env/bin/python monitor/real_ai_new_monitor.py > real_ai_new_monitor_log.txt 2>&1 &
fi

ps -ef | grep 'monitor/comm_news_monitor.py' | grep -v grep &> /dev/null
if [ $? -eq 0 ];then
  echo "存在monitor/comm_news_monitor.py程序"
else
  echo "启动monitor/comm_news_monitor.py程序"
  cd /Users/alpha/Documents/GitHub/QT
  nohup /Users/alpha/opt/anaconda3/envs/sklearn-env/bin/python monitor/comm_news_monitor.py > comm_news_monitor_log.txt 2>&1 &
fi