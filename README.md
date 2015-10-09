# iplant_border_msgserver
iPlant Border Message Server

Overview
--------
This program receives filesystem event messages from iPlant DataStore and translates to a new message form. Reproduced messages are sent to a separated exchange per a user so that each user can only access their messages.

Dependencies
------------
This program has dependencies following:
- JsonCpp
- Log4cxx
- RabbitMQ
- RabbitMQ-c
- libirodsfs (under irodsfs/)
- iRODS network plugins (under irodsfs/plugins_ARCH/network)


Build
-----
This program is created using "NetBeans IDE". To build, type:
```
make
```

Reference
---------
LRU cache code that the project uses is written by [SAURAV MOHAPATRA](https://github.com/mohaps/lrucache).
