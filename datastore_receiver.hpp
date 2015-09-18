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

typedef struct _DataStoreConn {
    char hostname[256];
    int port;
    char user_id[64];
    char user_pwd[64];
    char exchange[64];
} DataStoreConn_t;

typedef struct _DataStoreMsgReceiver {
    pthread_t thread;
    bool thread_run;
    DataStoreConn_t conn;
    amqp_socket_t *socket;
    amqp_connection_state_t conn_state;
    amqp_channel_t channel;
    amqp_bytes_t queuename;
} DataStoreMsgReceiver_t;

typedef struct _DataStoreMsg {
    unsigned long fdId;
    char *iRodsPath;
    off_t offset;
    size_t size;
    char *buffer;
} DataStoreMsg_t;

int readDataStoreMsgReceiverConf(char *path, DataStoreConn_t **conn);
int createDataStoreMsgReceiver(DataStoreConn_t *conn, DataStoreMsgReceiver_t **receiver);
int releaseDataStoreMsgReceiver(DataStoreMsgReceiver_t *receiver);
int runDataStoreMsgReceiver(DataStoreMsgReceiver_t *receiver);

#endif  /* DATASTORE_RECEIVER_HPP */