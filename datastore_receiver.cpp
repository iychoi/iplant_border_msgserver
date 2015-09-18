/* 
 * File:   datastore_receiver.cpp
 * Author: iychoi
 *
 * Created on September 17, 2015, 3:47 PM
 */

#include <cstdlib>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#include <log4cxx/logger.h>
#include <jsoncpp/json/value.h>
#include <jsoncpp/json/reader.h>
#include <iostream>
#include <fstream>
#include "receiver.hpp"
#include "datastore_receiver.hpp"

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("datastore_receiver"));

/*
 * Receive messages from iplant datastore and send to processor through receiver
 */
static void* _receiveThread(void* param) {
    DataStoreMsgReceiver_t *receiver = (DataStoreMsgReceiver_t *)param;
    amqp_envelope_t envelope;
    amqp_rpc_reply_t reply;
    GenericMsg_t *gmsg = NULL;
    
    assert(receiver != NULL);
    
    LOG4CXX_DEBUG(logger, "_receiveThread: event receiver thread started");
    
    while(1) {
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
        
        amqp_maybe_release_buffers(receiver->conn_state);
        
        reply = amqp_consume_message(receiver->conn_state, &envelope, NULL, 0);
        if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
            LOG4CXX_ERROR(logger, "_receiveThread: failed to consume message - killing the thread");
            break;
        }
        
        gmsg = (GenericMsg_t *)calloc(1, sizeof(GenericMsg_t));
        if(gmsg == NULL) {
            LOG4CXX_ERROR(logger, "_receiveThread: not enough memory to allocate");
            break;
        }
        
        gmsg->exchange = (char *)calloc(envelope.exchange.len + 1, 1);
        gmsg->routing_key = (char *)calloc(envelope.routing_key.len + 1, 1);
        gmsg->body = (char *)calloc(envelope.message.body.len + 1, 1);
        if(gmsg->exchange == NULL || gmsg->routing_key == NULL || gmsg->body == NULL) {
            LOG4CXX_ERROR(logger, "_receiveThread: not enough memory to allocate");
            break;
        }
        
        gmsg->delivery_tag = envelope.delivery_tag;
        gmsg->exchange_len = envelope.exchange.len;
        memcpy(gmsg->exchange, (char *)envelope.exchange.bytes, envelope.exchange.len);
        gmsg->routing_key_len = envelope.routing_key.len;
        memcpy(gmsg->routing_key, (char *)envelope.routing_key.bytes, envelope.routing_key.len);
        gmsg->body_len = envelope.message.body.len;
        memcpy(gmsg->body, (char *)envelope.message.body.bytes, envelope.message.body.len);
        
        LOG4CXX_DEBUG(logger, "_receiveThread: " << gmsg->delivery_tag << ":" << gmsg->exchange << ":" << gmsg->routing_key << "\t" << gmsg->body);
        
        receive(gmsg);
        
        gmsg = NULL;
        
        amqp_destroy_envelope(&envelope);
        
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    }
    
    LOG4CXX_DEBUG(logger, "_receiveThread: event receiver thread terminated for unknown reason");
    receiver->thread_run = false;
}

static int _checkConnConf(DataStoreConn_t *conn) {
    if(conn == NULL) {
        LOG4CXX_ERROR(logger, "_checkConnConf: conn is null");
        return EINVAL;
    }
    
    if(strlen(conn->hostname) == 0) {
        LOG4CXX_ERROR(logger, "_checkConnConf: conn.hostname is empty");
        return EINVAL;
    }
    
    if(conn->port <= 0) {
        LOG4CXX_ERROR(logger, "_checkConnConf: conn.port is negative");
        return EINVAL;
    }
    
    if(strlen(conn->user_id) == 0) {
        LOG4CXX_ERROR(logger, "_checkConnConf: conn.user_id is empty");
        return EINVAL;
    }
    
    if(strlen(conn->user_pwd) == 0) {
        LOG4CXX_ERROR(logger, "_checkConnConf: conn.user_pwd is empty");
        return EINVAL;
    }
    
    if(strlen(conn->exchange) == 0) {
        LOG4CXX_ERROR(logger, "_checkConnConf: conn.exchange is empty");
        return EINVAL;
    }
    
    return 0;
}

int readDataStoreMsgReceiverConf(char *path, DataStoreConn_t **conn) {
    DataStoreConn_t *handle;
    Json::Value conf;
    Json::Reader reader;
    ifstream istream;
    
    if(path == NULL) {
        LOG4CXX_ERROR(logger, "readDataStoreMsgReceiverConf: path is null");
        return EINVAL;
    }
    
    if(conn == NULL) {
        LOG4CXX_ERROR(logger, "readDataStoreMsgReceiverConf: conn is null");
        return EINVAL;
    }
    
    istream.open(path);
    
    bool parsed = reader.parse(istream, conf, false);
    if(!parsed) {
        LOG4CXX_ERROR(logger, "readDataStoreMsgReceiverConf: unable to parse configuration file");
        return EINVAL;
    }
    
    *conn = NULL;
    
    handle = (DataStoreConn_t *)calloc(1, sizeof(DataStoreConn_t));
    if(handle == NULL) {
        LOG4CXX_ERROR(logger, "readDataStoreMsgReceiverConf: not enough memory to allocate");
        return ENOMEM;
    }
    
    strcpy(handle->hostname, conf["hostname"].asCString());
    handle->port = conf["port"].asInt();
    strcpy(handle->user_id, conf["user_id"].asCString());
    strcpy(handle->user_pwd, conf["user_pwd"].asCString());
    strcpy(handle->exchange, conf["exchange"].asCString());
    
    *conn = handle;
    
    return 0;
}

int createDataStoreMsgReceiver(DataStoreConn_t *conn, DataStoreMsgReceiver_t **receiver) {
    int status = 0;
    DataStoreMsgReceiver_t *handle;
    amqp_rpc_reply_t reply;
    amqp_queue_declare_ok_t *queue_status;
    
    status = _checkConnConf(conn);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: connection configuration check failed");
        return status;
    }
    
    if(receiver == NULL) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: receiver is null");
        return EINVAL;
    }
    
    *receiver = NULL;
    
    handle = (DataStoreMsgReceiver_t *)calloc(1, sizeof(DataStoreMsgReceiver_t));
    if(handle == NULL) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: not enough memory to allocate");
        return ENOMEM;
    }
    
    memcpy(&handle->conn, conn, sizeof(DataStoreConn_t));
    handle->thread_run = false;
    
    LOG4CXX_DEBUG(logger, "createDataStoreMsgReceiver: creating a TCP connection to " << handle->conn.hostname);
    
    // create a TCP connection
    handle->conn_state = amqp_new_connection();
    handle->socket = amqp_tcp_socket_new(handle->conn_state);
    if(handle->socket == NULL) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: unable to create a connection");
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }
    
    // open a socket
    status = amqp_socket_open(handle->socket, handle->conn.hostname, handle->conn.port);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: unable to create a TCP connection to " << handle->conn.hostname);
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }
    
    LOG4CXX_DEBUG(logger, "createDataStoreMsgReceiver: logging in with " << handle->conn.user_id);
    
    // login
    reply = amqp_login(handle->conn_state, "/", 0, AMQP_DEFAULT_FRAME_SIZE, 0, AMQP_SASL_METHOD_PLAIN, handle->conn.user_id, handle->conn.user_pwd);
    if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: unable to login with " << handle->conn.user_id);
        amqp_connection_close(handle->conn_state, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }
    
    LOG4CXX_DEBUG(logger, "createDataStoreMsgReceiver: opening a channel");
    
    // open a channel
    handle->channel = 1;
    amqp_channel_open(handle->conn_state, handle->channel);
    reply = amqp_get_rpc_reply(handle->conn_state);
    if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: unable to open a channel");
        amqp_connection_close(handle->conn_state, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }

    LOG4CXX_DEBUG(logger, "createDataStoreMsgReceiver: declaring and binding a queue");
    
    // declare a queue
    queue_status = amqp_queue_declare(handle->conn_state, handle->channel, amqp_empty_bytes, 0, 0, 1, 1, amqp_empty_table);
    reply = amqp_get_rpc_reply(handle->conn_state);
    if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: unable to declare a queue");
        amqp_channel_close(handle->conn_state, handle->channel, AMQP_REPLY_SUCCESS);
        amqp_connection_close(handle->conn_state, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }
    
    handle->queuename = amqp_bytes_malloc_dup(queue_status->queue);
    if(handle->queuename.bytes == NULL) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: unable to declare a queue");
        amqp_channel_close(handle->conn_state, handle->channel, AMQP_REPLY_SUCCESS);
        amqp_connection_close(handle->conn_state, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }
    
    // bind a queue
    amqp_queue_bind(handle->conn_state, handle->channel, handle->queuename, amqp_cstring_bytes(handle->conn.exchange), amqp_cstring_bytes("#"), amqp_empty_table);
    reply = amqp_get_rpc_reply(handle->conn_state);
    if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: unable to bind a queue");
        amqp_channel_close(handle->conn_state, handle->channel, AMQP_REPLY_SUCCESS);
        amqp_connection_close(handle->conn_state, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }
    
    LOG4CXX_DEBUG(logger, "createDataStoreMsgReceiver: starting consuming");
    
    // start consume
    amqp_basic_consume(handle->conn_state, handle->channel, handle->queuename, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
    reply = amqp_get_rpc_reply(handle->conn_state);
    if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: unable to start consuming");
        amqp_channel_close(handle->conn_state, handle->channel, AMQP_REPLY_SUCCESS);
        amqp_connection_close(handle->conn_state, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }
    
    
    *receiver = handle;
    return 0;
}

int releaseDataStoreMsgReceiver(DataStoreMsgReceiver_t *receiver) {
    if(receiver == NULL) {
        LOG4CXX_ERROR(logger, "releaseDataStoreMsgReceiver: receiver is null");
        return EINVAL;
    }
    
    if(receiver->thread_run) {
        // stop thread
        LOG4CXX_DEBUG(logger, "releaseDataStoreMsgReceiver: canceling an event receiver thread");
        pthread_cancel(receiver->thread);
        
        receiver->thread_run = false;
    }
    
    amqp_channel_close(receiver->conn_state, receiver->channel, AMQP_REPLY_SUCCESS);
    amqp_connection_close(receiver->conn_state, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(receiver->conn_state);
    
    free(receiver);
    LOG4CXX_DEBUG(logger, "releaseDataStoreMsgReceiver: closed connection");
    return 0;
}

int runDataStoreMsgReceiver(DataStoreMsgReceiver_t *receiver) {
    int status = 0;
    
    if(receiver == NULL) {
        LOG4CXX_ERROR(logger, "runDataStoreMsgReceiver: receiver is null");
        return EINVAL;
    }
    
    if(receiver->thread_run) {
        LOG4CXX_ERROR(logger, "runDataStoreMsgReceiver: an event receiver thread is running");
        return EINVAL;
    }
    
    status = _checkConnConf(&receiver->conn);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "runDataStoreMsgReceiver: connection configuration check failed");
        return status;
    }
    
    // create a thread
    status = pthread_create(&receiver->thread, NULL, _receiveThread, (void*)receiver);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "runDataStoreMsgReceiver: unable to create an event receiver thread");
        return status;
    }
    
    receiver->thread_run = true;
    LOG4CXX_DEBUG(logger, "runDataStoreMsgReceiver: an event receiver thread is started");
    return 0;
}