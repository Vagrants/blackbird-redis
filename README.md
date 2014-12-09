blackbird-redis
===============

get redis info by using telnetlib.


config file
-----------

| name                    | default        | type                | notes                               |
|-------------------------|----------------|---------------------|-------------------------------------|
| host                    | 127.0.0.1      | string              | redis host                          |
| port                    | 6379           | interger(1 - 65535) | redis lisetn port                   |
| response_check_key      | __zabbix_check | string              | set/get key name for response check |
| db                      | 0              | interger(0 - 15)    | select db number                    |
| auth                    | None           | string              | only require option                 |

Please see the "redis.cfg".
