# Trino on RocketMQ
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

Trino:  [文档中心](https://trino.io/)

RocketMQ:  [文档中心](https://rocketmq.apache.org/)
# 使用场景参考
```
[英] https://www.uber.com/blog/presto-on-apache-kafka-at-uber-scale/
[中] https://cloud.tencent.com/developer/article/1983280
```
# 架构介绍
>扩展 trino rocketmq connector，用于支持 rocketmq 下面的即席查询

![img_1.png](img_1.png)
# 快速开始(基于docker)
## 环境准备
> [ 1.] Linux/Unix/Mac
> 
> [ 2.] 64bit JDK 17
> 
> [ 3.] RocketMQ搭建 ，具体参考 [RocketMQ环境搭建](https://rocketmq.apache.org/docs/%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8/02quickstart)
>
> [ 4.] Docker镜像运行环境

## 下载docker镜像
```
docker pull trinodb/trino 
```
## 编译打包

```
git clone git@github.com:sunxiaojian/trino-rocketmq.git

cd trino-rocketmq/
 
mvn clean package -Dmaven.test.skip=true
```
## 配置目录

#### 准备配置目录
```
mkdir $PWD/trino
mkdir $PWD/trino/etc
mkdir $PWD/trino/plugin
mkdir $PWD/trino/plugin/rocketmq
```
#### 准备插件
将之前编译好的插件copy到rocketmq插件目录下面 
```
mv target/trino-rocketmq-397/* $PWD/trino/plugin/rocketmq
mv target/trino-rocketmq-397.jar $PWD/trino/plugin/rocketmq
```
#### 增加配置
**配置文件存放在 $PWD/trino/etc 路径下**
> 参考配置 [catalog](https://github.com/sunxiaojian/trino-rocketmq/tree/main/config)

## 启动

```
 docker run --name trino -d
            -p 8080:8080    ## 访问端口
            -p 5005:5005    ## 开启debug
            --volume $PWD/trino/etc:/etc/trino   ## 配置挂载目录
            --volume $PWD/trino/plugin:/usr/lib/trino/plugin  ## 插件挂载目录
            trinodb/trino  
```

## 测试
```
docker exec -it trino trino --catalog rocketmq --schema default
trino:default> show tables;
  Table
----------
 customer
(1 row)

Query 20220930_071816_00012_aem7m, FINISHED, 1 node
Splits: 7 total, 7 done (100.00%)
0.36 [1 rows, 25B] [2 rows/s, 70B/s]

trino:default> select * from customer;
 _queue_id | _queue_offset | _message | _message_length | _key | _key_length | _timestamp | _properties
-----------+---------------+----------+-----------------+------+-------------+------------+-------------
(0 rows)

Query 20220930_071847_00013_aem7m, FINISHED, 1 node
Splits: 8 total, 8 done (100.00%)
0.60 [0 rows, 0B] [0 rows/s, 0B/s]
```

## 基于 file schema 测试
### 创建topic
```
sh mqadmin updateTopic  -t customers -n 127.0.0.1:9876 -b 127.0.0.1:10911
```
### 添加测试数据
```
sh mqadmin sendMessage  -b 127.0.0.1:10911 -t  customers -p '{"rowNumber":1,"customerKey":1,"name":"Customer#000000001","address":"IVhzIApeRb ot,c,E","nationKey":15,"phone":"25-989-741-2988","accountBalance":711.56,"marketSegment":"BUILDING","comment":"to the even, regular platelets. regular, ironic epitaphs nag e"}'
sh mqadmin sendMessage  -b 127.0.0.1:10911 -t  customers -p '{"rowNumber":3,"customerKey":3,"name":"Customer#000000003","address":"MG9kdTD2WBHm","nationKey":1,"phone":"11-719-748-3364","accountBalance":7498.12,"marketSegment":"AUTOMOBILE","comment":" deposits eat slyly ironic, even instructions. express foxes detect slyly."}'
sh mqadmin sendMessage  -b 127.0.0.1:10911 -t  customers -p '{"rowNumber":5,"customerKey":5,"name":"Customer#000000005","address":"KvpyuHCplrB84WgAiGV6sYpZq7Tj","nationKey":3,"phone":"13-750-942-6364","accountBalance":794.47,"marketSegment":"HOUSEHOLD","comment":"n accounts will have to unwind. foxes cajole accor"}'
sh mqadmin sendMessage  -b 127.0.0.1:10911 -t  customers -p '{"rowNumber":7,"customerKey":7,"name":"Customer#000000007","address":"TcGe5gaZNgVePxU5kRrvXBfkasDTea","nationKey":18,"phone":"28-190-982-9759","accountBalance":9561.95,"marketSegment":"AUTOMOBILE","comment":"ainst the ironic, express theodolites. express, even pinto bean"}'
sh mqadmin sendMessage  -b 127.0.0.1:10911 -t  customers -p '{"rowNumber":9,"customerKey":9,"name":"Customer#000000009","address":"xKiAFTjUsCuxfeleNqefumTrjS","nationKey":8,"phone":"18-338-906-3675","accountBalance":8324.07,"marketSegment":"FURNITURE","comment":"r theodolites according to the requests wake thinly excuses: pending"}'

```
### 添加配置文件 vim customer.json
```
{
    "tableName": "customers",
    "topicName": "customers",
    "message": {
        "dataFormat": "json",
        "fields": [
            {
                "name": "rowNumber",
                "mapping": "rowNumber",
                "type": "BIGINT"
            },
            {
                "name": "customerKey",
                "mapping": "customerKey",
                "type": "BIGINT"
            },
            {
                "name": "name",
                "mapping": "name",
                "type": "VARCHAR"
            },
            {
                "name": "address",
                "mapping": "address",
                "type": "VARCHAR"
            },
            {
                "name": "nationKey",
                "mapping": "nationKey",
                "type": "BIGINT"
            },
            {
                "name": "phone",
                "mapping": "phone",
                "type": "VARCHAR"
            },
            {
                "name": "accountBalance",
                "mapping": "accountBalance",
                "type": "DOUBLE"
            },
            {
                "name": "marketSegment",
                "mapping": "marketSegment",
                "type": "VARCHAR"
            },
            {
                "name": "comment",
                "mapping": "comment",
                "type": "VARCHAR"
            }
        ]
    }
}
```
### 启动并挂载配置
```
docker run --name trino -d -p 8080:8080 -p 5005:5005  
      --volume $PWD/trino/etc:/etc/trino  
      --volume $PWD/trino/plugin:/usr/lib/trino/plugin 
      --volume $PWD/trino/etc/rocketmq:/etc/rocketmq   
      trinodb/trino
```
### 测试
```
trino@f7e89ebd3594:/$ trino --catalog rocketmq --schema default
trino:default> show tables;
   Table
-----------
 customers
(1 row)

Query 20221014_115646_00005_ez64t, FINISHED, 1 node
Splits: 7 total, 7 done (100.00%)
0.24 [1 rows, 26B] [4 rows/s, 110B/s]

trino:default> describe table customers;
Query 20221014_115702_00007_ez64t failed: line 1:10: mismatched input 'table'. Expecting: 'INPUT', 'OUTPUT', <identifier>
describe table customers

trino:default> describe customers;
     Column      |     Type     | Extra |                    Comment
-----------------+--------------+-------+------------------------------------------------
 rownumber       | bigint       |       |
 customerkey     | bigint       |       |
 name            | varchar      |       |
 address         | varchar      |       |
 nationkey       | bigint       |       |
 phone           | varchar      |       |
 accountbalance  | double       |       |
 marketsegment   | varchar      |       |
 comment         | varchar      |       |
 _queue_id       | bigint       |       | Queue Id
 _broker_name    | varchar      |       | Broker name
 _queue_offset   | bigint       |       | Offset for the message within the MessageQueue
 _message        | varchar      |       | Message text
 _message_length | bigint       |       | Total number of message bytes
 _key            | varchar      |       | Key text
 _key_length     | bigint       |       | Total number of key bytes
 _timestamp      | timestamp(3) |       | Message timestamp
(17 rows)

Query 20221014_115707_00008_ez64t, FINISHED, 1 node
Splits: 7 total, 7 done (100.00%)
0.37 [17 rows, 1.26KB] [45 rows/s, 3.36KB/s]

trino:default> select * from customers;
 rownumber | customerkey |        name        |            address             | nationkey |      phone      | accountbalance | marketsegment |                                      comment                                   >
-----------+-------------+--------------------+--------------------------------+-----------+-----------------+----------------+---------------+-------------------------------------------------------------------------------->
         1 |           1 | Customer#000000001 | IVhzIApeRb ot,c,E              |        15 | 25-989-741-2988 |         711.56 | BUILDING      | to the even, regular platelets. regular, ironic epitaphs nag e                 >
         5 |           5 | Customer#000000005 | KvpyuHCplrB84WgAiGV6sYpZq7Tj   |         3 | 13-750-942-6364 |         794.47 | HOUSEHOLD     | n accounts will have to unwind. foxes cajole accor                             >
         7 |           7 | Customer#000000007 | TcGe5gaZNgVePxU5kRrvXBfkasDTea |        18 | 28-190-982-9759 |        9561.95 | AUTOMOBILE    | ainst the ironic, express theodolites. express, even pinto bean                >
         9 |           9 | Customer#000000009 | xKiAFTjUsCuxfeleNqefumTrjS     |         8 | 18-338-906-3675 |        8324.07 | FURNITURE     | r theodolites according to the requests wake thinly excuses: pending           >
         3 |           3 | Customer#000000003 | MG9kdTD2WBHm                   |         1 | 11-719-748-3364 |        7498.12 | AUTOMOBILE    |  deposits eat slyly ironic, even instructions. express foxes detect slyly. blit>
(5 rows)
```

## 基于 rocketmq schema registry 测试

```
 待支持
```


