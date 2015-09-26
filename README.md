# iplant_border_msgserver
An iPlant Border Message Server

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

Build
-----
This program is created using "NetBeans IDE". To build, type:
```
make
```

