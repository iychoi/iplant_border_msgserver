/* 
 * File:   datastore_receiver.hpp
 * Author: iychoi
 *
 * Created on September 17, 2015, 3:47 PM
 */
#ifndef DATASTORE_RECEIVER_HPP
#define DATASTORE_RECEIVER_HPP

#include <pthread.h>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#include "common.hpp"
#include "publisher.hpp"

#define MESSAGE_BODY_MAX_LEN  4096

typedef struct _DataStoreMsgServerConf {
    char hostname[HOSTNAME_MAX_LEN];
    int port;
    char user_id[CREDENTIAL_MAX_LEN];
    char user_pwd[CREDENTIAL_MAX_LEN];
    char vhost[CREDENTIAL_MAX_LEN];
    char exchange[CREDENTIAL_MAX_LEN];
    char **routing_keys;
    int routing_keys_len;
} DataStoreMsgServerConf_t;

typedef struct _DataStoreMsgReceiver {
    pthread_t thread;
    bool thread_run;
    amqp_socket_t *socket;
    amqp_connection_state_t conn_state;
    amqp_channel_t channel;
    amqp_bytes_t queuename;
    Publisher_t *publisher;
} DataStoreMsgReceiver_t;

typedef struct _DataStoreMsg {
    char zone[CREDENTIAL_MAX_LEN];
    char name[CREDENTIAL_MAX_LEN];
    char operation[OPERATION_MAX_LEN];
    char body[MESSAGE_BODY_MAX_LEN];
    struct _DataStoreMsg *next;
} DataStoreMsg_t;

int readDataStoreMsgReceiverConf(char *path, DataStoreMsgServerConf_t **conf);
int releaseDataStoreMsgReceiverConf(DataStoreMsgServerConf_t *conf);
int createDataStoreMsgReceiver(DataStoreMsgServerConf_t *conf, Publisher_t *publisher, DataStoreMsgReceiver_t **receiver);
int releaseDataStoreMsgReceiver(DataStoreMsgReceiver_t *receiver);
int runDataStoreMsgReceiver(DataStoreMsgReceiver_t *receiver);

#endif  /* DATASTORE_RECEIVER_HPP */