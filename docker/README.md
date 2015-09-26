# iPlant Border Message Server Docker Image

Build Docker Image
------------------

Run config.py first to generate configurations that message server needs.
```
python config.py
```

Then, build docker image using Dockerfile.
```
sudo docker build -t iplant_msg_server .
```


Launch Message Server
---------------------

Run docker.
```
sudo docker run --rm=true --name iplant_msg_svr -p 15672:15672 -p 5672:5672 iplant_msg_server
```
