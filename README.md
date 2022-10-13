# Trino on RocketMQ
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

Trino:  [文档中心](https://trino.io/)

RocketMQ:  [文档中心](https://rocketmq.apache.org/)
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
### 创建topic并添加数据

### 添加配置文件 vim customer.json
```
{
    "tableName": "customer",
    "topicName": "customer",
    "key": {
        "dataFormat": "raw",
        "fields": [
            {
                "name": "key",
                "dataFormat": "LONG",
                "type": "BIGINT",
                "hidden": "false"
            }
        ]
    },
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
trino@214be12d239f:/$ trino --catalog rocketmq --schema default
trino:default> show tables;
  Table
----------
 customer
(1 row)

Query 20221013_125244_00002_uted7, FINISHED, 1 node
Splits: 7 total, 7 done (100.00%)
0.26 [1 rows, 25B] [3 rows/s, 98B/s]

trino:default> DESCRIBE customer;
     Column      |              Type              | Extra |                    Comment
-----------------+--------------------------------+-------+------------------------------------------------
 key             | bigint                         |       |
 rownumber       | bigint                         |       |
 customerkey     | bigint                         |       |
 name            | varchar                        |       |
 address         | varchar                        |       |
 nationkey       | bigint                         |       |
 phone           | varchar                        |       |
 accountbalance  | double                         |       |
 marketsegment   | varchar                        |       |
 comment         | varchar                        |       |
 _queue_id       | bigint                         |       | Queue Id
 _queue_offset   | bigint                         |       | Offset for the message within the MessageQueue
 _message        | varchar                        |       | Message text
 _message_length | bigint                         |       | Total number of message bytes
 _key            | varchar                        |       | Key text
 _key_length     | bigint                         |       | Total number of key bytes
 _timestamp      | timestamp(3)                   |       | Message timestamp
 _properties     | map(varchar, array(varbinary)) |       | message properties
(18 rows)

Query 20221013_125300_00003_uted7, FINISHED, 1 node
Splits: 7 total, 7 done (100.00%)
0.43 [18 rows, 1.33KB] [42 rows/s, 3.12KB/s]

trino:default> select * from customer limit 3;
Query 20221013_125652_00004_uted7 failed: org.apache.rocketmq.remoting.exception.RemotingConnectException: connect to [30.240.80.192:9876] failed


```

## 基于 rocketmq schema registry 测试

```
 待支持
```


