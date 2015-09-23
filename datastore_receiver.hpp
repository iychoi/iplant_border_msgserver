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

#define MESSAGE_BODY_MAX_LEN  4096

typedef struct _DataStoreConf {
    char hostname[HOSTNAME_MAX_LEN];
    int port;
    char user_id[CREDENTIAL_MAX_LEN];
    char user_pwd[CREDENTIAL_MAX_LEN];
    char exchange[CREDENTIAL_MAX_LEN];
    char **routing_keys;
    int routing_keys_len;
} DataStoreConf_t;

typedef struct _DataStoreMsgReceiver {
    pthread_t thread;
    bool thread_run;
    amqp_socket_t *socket;
    amqp_connection_state_t conn_state;
    amqp_channel_t channel;
    amqp_bytes_t queuename;
} DataStoreMsgReceiver_t;

typedef struct _DataStoreMsg {
    char zone[CREDENTIAL_MAX_LEN];
    char name[CREDENTIAL_MAX_LEN];
    char operation[OPERATION_MAX_LEN];
    char body[MESSAGE_BODY_MAX_LEN];
} DataStoreMsg_t;

int readDataStoreMsgReceiverConf(char *path, DataStoreConf_t **conf);
int releaseDataStoreMsgReceiverConf(DataStoreConf_t *conf);
int createDataStoreMsgReceiver(DataStoreConf_t *conf, DataStoreMsgReceiver_t **receiver);
int releaseDataStoreMsgReceiver(DataStoreMsgReceiver_t *receiver);
int runDataStoreMsgReceiver(DataStoreMsgReceiver_t *receiver);

#endif  /* DATASTORE_RECEIVER_HPP */