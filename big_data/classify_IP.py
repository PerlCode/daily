# -*- coding:utf-8 -*-
###################### classify.py #############################
# 文件类型：python源程序
# 基本功能：BGP信息简单处理
# 开发平台：spark
# 编    者：吕金航
# 学    号：1017030913
# 完成日期: 20171028
# 任务号/BUG号:
# 概要/详设文档/变更号:
# 日期,修改者,任务号/BUG号,概要/详设文档/变更号,函数名,功能描述
###############################################################
from IPy import IPSet, IP
import sys
from pyspark.sql import SparkSession
import re
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import math
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt

def handle(data):
    reip = re.compile(r'(?<![\.\d])(?:\d{1,3}\.){3}\d{1,3}\/\d+')
    l = reip.findall(data)
    ips = []
    print("*************\n\n")
    for all_prefixes in l:
        ip = IP(all_prefixes)
        for x in ip:
        	ips.append(x)
    return ips
    


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    appName ="jhl_spark_1"
    master = "local"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    # textFile读取外部数据

    rdd = sc.textFile(sys.argv[1])# 以行为单位读取外部文件，并转化为RDD
    
    rdd2 = rdd.flatMap(handle) \
              .distinct()
    result = rdd2.collect()
    print("********************************\n\n")
    ret = IPSet()
    for y in result:
        ret.add(IP(y, make_net = True))
    #print(ret)#合并后的网段
        
    
    dict = {}
    for z in result:
        dec = IP(z).int()
        dict[z] = dec
    a_start = IP('1.0.0.0').int()
    a_end = IP('126.0.0.0').int()
    b_start = IP('128.0.0.0').int()
    b_end = IP('191.255.255.255').int()
    c_start = IP('192.0.0.0').int()
    c_end = IP('223.255.255.255').int() 
    a_count = 0
    b_count = 0
    c_count = 0
    for key in dict:
        if dict[key]<=a_end :
            a_count += 1
        elif dict[key] >= b_start and dict[key] <= b_end :
        	b_count += 1
        elif dict[key] >= c_start and dict[key] <= c_end :
        	c_count += 1
    
    #画图
    
    data = [int(a_count),int(b_count),int(c_count)]
    labels = ['A','B','C']
    plt.bar(range(len(data)),data,tick_label=labels)
    plt.title("IPAddress_classification")
    plt.xlabel("kinds of IPaddress")
    plt.ylabel("number")
#    plt.show()
    plt.savefig("IPAddress_classification.jpg") 
    plt.close()
    
    f1 = open('combined_IPaddress.txt','w')
    for ip in ret:
        f1.write(ip.strNormal(1))
        f1.write("\n")
    f1.write("\n")
    f1.write("The Number of A kind IP:"+str(a_count)+"\n")
    f1.write("The Number of B kind IP:"+str(b_count)+"\n")
    f1.write("The Number of C kind IP:"+str(c_count)+"\n")
    f1.close()
    
    sc.stop()