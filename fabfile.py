from __future__ import with_statement
from fabric.api import *
import os

#env.hosts = ['limgad.homeunix.net:2020']
#env.shell = "/bin/sh -c"
env.shell = "/usr/bin/env bash -c"

HADOOP_HOME = ''
HADOOP_CONF_DIR = ''
TARGET=''

def hadoop_config(hosts=''):
    global HADOOP_HOME 
    global HADOOP_CONF_DIR 
    HADOOP_HOME = '~/hadoop' #os.path.dirname(env.real_fabfile)
    HADOOP_CONF_DIR = HADOOP_HOME+"/conf"

def parse_hosts(hostfile):
    filehandler = open(os.path.expanduser(HADOOP_CONF_DIR + '/' + hostfile), 'r')
    temp_hosts = []
    for line in filehandler.readlines():
        if line.startswith('#'):
            continue
        temp_hosts.append(line.replace('\n', '').replace(' -p ', ':'))

    return temp_hosts
            

#def hadoop_env(hosts=''):
#    JAVA_HOME = '/usr/local/diablo-jdk1.6.0'
#    HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_NAMENODE_OPTS"
#    HADOOP_SECONDARYNAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_SECONDARYNAMENODE_OPTS"
#    HADOOP_DATANODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_DATANODE_OPTS"
#    HADOOP_BALANCER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_BALANCER_OPTS"
#    HADOOP_JOBTRACKER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_JOBTRACKER_OPTS"

def masters():
    global TARGET
    hadoop_config()
    env.hosts = parse_hosts('masters')
    TARGET = 'masters'

def slaves():
    global TARGET
    hadoop_config()
    env.hosts = parse_hosts('slaves')
    TARGET = 'slaves'

def slaves_do(command):
    run("cd "+HADOOP_HOME+"; ./bin/hadoop-daemon.sh --config "+HADOOP_CONF_DIR+" "+command)

def hadoop_daemons(command, hosts=''):
    slaves_do(command)

def start():
    # Start dfs
    namenode('start')
    datanode('start')

    # Start mapred
    jobtracker('start')
    tasktracker('start')
    
def jobtracker(action):
    if TARGET != 'masters':
        return
    local(HADOOP_HOME+'/bin/hadoop-daemon.sh --config ' +HADOOP_CONF_DIR+ ' ' + action + ' jobtracker', capture=False)

def tasktracker(action):
    if TARGET != 'slaves':
        return
    hadoop_daemons(action + ' tasktracker')

def namenode(action):
    if TARGET != 'masters':
        return
    local(HADOOP_HOME+'/bin/hadoop-daemon.sh --config '+HADOOP_CONF_DIR+ ' ' + action +' namenode', capture=False)
    hadoop_daemons(action + ' secondarynamenode')#, hosts='masters')

def datanode(action):
    if TARGET != 'slaves':
        return
    hadoop_daemons(action + ' datanode')

def stop():
    # Stop dfs
    namenode('stop')
    datanode('stop')

    # Stop mapred
    jobtracker('stop')
    tasktracker('stop')

def test():
    run('hostname')
