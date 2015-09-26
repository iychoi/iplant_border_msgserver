# iPlant Border Message Server
#
# VERSION	1.0

FROM	ubuntu:14.04
MAINTAINER	Illyoung Choi <iychoi@email.arizona.edu>

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update
RUN apt-get install -y wget

##############################################
# Log4cxx
##############################################
RUN apt-get install -y liblog4cxx10


##############################################
# JsonCpp
##############################################
RUN apt-get install -y libjsoncpp0


##############################################
# RabbitMQ-C
##############################################
ADD dependencies/rabbitmq-c_0.7.0-1_amd64.deb /rabbitmq-c_0.7.0-1_amd64.deb
RUN dpkg -i rabbitmq-c_0.7.0-1_amd64.deb


##############################################
# Border Message Server
##############################################
ADD messageserver /opt/messageserver
RUN chmod +x /opt/messageserver/iplant_border_msg_server
RUN chmod +x /opt/messageserver/Debug/iplant_border_msg_server
RUN chmod +x /opt/messageserver/Release/iplant_border_msg_server


##############################################
# RabbitMQ
##############################################
RUN apt-get install -y rabbitmq-server

RUN rabbitmq-plugins enable rabbitmq_management

# expose rabbitmq_management user interface (port 15672) and rabbitmq service interface (port 5672)
EXPOSE 15672
EXPOSE 5672

# script
ADD initService.sh /initService.sh
RUN chmod +x /initService.sh

CMD ["/initService.sh"]

