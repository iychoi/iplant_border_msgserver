#!/bin/sh

( sleep 10 ; \

# Create users
rabbitmqctl add_user iplant iplant ; \

# Set user rights
rabbitmqctl set_user_tags iplant administrator ; \
rabbitmqctl set_permissions -p / iplant ".*" ".*" ".*" ; \

# Run message server
export IRODS_PLUGINS_HOME="/opt/messageserver/irodsfs/plugins/"
cd /opt/messageserver/ && ./iplant_border_msg_server ; \

) &

# Run RabbitMQ
rabbitmq-server

