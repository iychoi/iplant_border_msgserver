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
const char *accept[] = {
    "collection.add",
    "collection.rm",
    "collection.acl.mod",
    "data-object.add",
    "data-object.rm",
    "data-object.mod",
    "data-object.acl.mod"
};

/*
 * Receive messages from iplant datastore and send to processor through receiver
 */
static bool _checkAccept(amqp_envelope_t *envelope) {
    char routing_key[1024];
    int i;
    
    if(envelope == NULL) {
        LOG4CXX_ERROR(logger, "_checkAccept: envelope is null");
        return -EINVAL;
    }
    
    memcpy(routing_key, (char*)envelope->routing_key.bytes, envelope->routing_key.len);
    routing_key[envelope->routing_key.len] = 0;
    
    // check accept
    for(i=0;i<sizeof(accept);i++) {
        if(strcmp(routing_key, accept[i]) == 0) {
            return true;
        }
    }
    
    return false;
}

static int _process(amqp_envelope_t *envelope) {
    int status = 0;
    GenericMsg_t *gmsg = NULL;
    
    if(envelope == NULL) {
        LOG4CXX_ERROR(logger, "_process: envelope is null");
        return -EINVAL;
    }
    
    // pass to receiver to process
    status = createGenericMessage(envelope, &gmsg);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "_process: failed to convert to generic message - killing the thread");
        return EIO;
    }
    
    LOG4CXX_DEBUG(logger, "_process: " << gmsg->delivery_tag << ":" << gmsg->exchange << ":" << gmsg->routing_key << "\t" << gmsg->body);
    
    receive(gmsg);

    gmsg = NULL;
    return 0;
}

static void* _receiveThread(void* param) {
    int status = 0;
    DataStoreMsgReceiver_t *receiver = (DataStoreMsgReceiver_t *)param;
    amqp_envelope_t envelope;
    amqp_rpc_reply_t reply;
    
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
        
        if(_checkAccept(&envelope)) {
            status = _process(&envelope);
            if(status != 0) {
                LOG4CXX_ERROR(logger, "_receiveThread: failed to process message - killing the thread");
                break;
            }
        }
        
        amqp_destroy_envelope(&envelope);
        
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    }
    
    LOG4CXX_DEBUG(logger, "_receiveThread: event receiver thread terminated for unknown reason");
    receiver->thread_run = false;
}

static int _checkConnConf(DataStoreConf_t *conn) {
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

int readDataStoreMsgReceiverConf(char *path, DataStoreConf_t **conf) {
    DataStoreConf_t *handle;
    Json::Value confjson;
    Json::Reader reader;
    ifstream istream;
    Json::ArrayIndex arrsize;
    Json::Value arr;
    int i;
    
    if(path == NULL) {
        LOG4CXX_ERROR(logger, "readDataStoreMsgReceiverConf: path is null");
        return EINVAL;
    }
    
    if(conf == NULL) {
        LOG4CXX_ERROR(logger, "readDataStoreMsgReceiverConf: conf is null");
        return EINVAL;
    }
    
    istream.open(path);
    
    bool parsed = reader.parse(istream, confjson, false);
    if(!parsed) {
        LOG4CXX_ERROR(logger, "readDataStoreMsgReceiverConf: unable to parse configuration file");
        return EINVAL;
    }
    
    *conf = NULL;
    
    handle = (DataStoreConf_t *)calloc(1, sizeof(DataStoreConf_t));
    if(handle == NULL) {
        LOG4CXX_ERROR(logger, "readDataStoreMsgReceiverConf: not enough memory to allocate");
        return ENOMEM;
    }
    
    strcpy(handle->hostname, confjson["hostname"].asCString());
    handle->port = confjson["port"].asInt();
    strcpy(handle->user_id, confjson["user_id"].asCString());
    strcpy(handle->user_pwd, confjson["user_pwd"].asCString());
    strcpy(handle->exchange, confjson["exchange"].asCString());
    
    arrsize = confjson["routing_keys"].size();
    handle->routing_keys_len = arrsize;
    
    if(arrsize != 0) {
        handle->routing_keys = (char**)calloc(arrsize, sizeof(char*));
        if(handle->routing_keys == NULL) {
            LOG4CXX_ERROR(logger, "readDataStoreMsgReceiverConf: not enough memory to allocate");
            return ENOMEM;
        }

        arr = confjson["routing_keys"];
        for(i=0;i<arrsize;i++) {
            Json::Value val = arr.get(i, Json::Value::null);
            handle->routing_keys[i] = (char*)calloc(1, strlen(val.asCString())+1);
            if(handle->routing_keys[i] == NULL) {
                LOG4CXX_ERROR(logger, "readDataStoreMsgReceiverConf: not enough memory to allocate");
                return ENOMEM;
            }

            strcpy(handle->routing_keys[i], val.asCString());
        }
    }
    
    *conf = handle;
    
    return 0;
}

int releaseDataStoreMsgReceiverConf(DataStoreConf_t *conf) {
    if(conf == NULL) {
        LOG4CXX_ERROR(logger, "releaseDataStoreMsgReceiverConf: conf is null");
        return EINVAL;
    }
    
    if(conf->routing_keys != NULL) {
        int i;
        for(i=0;i<conf->routing_keys_len;i++) {
            if(conf->routing_keys[i] != NULL) {
                free(conf->routing_keys[i]);
                conf->routing_keys[i] = NULL;
            }
        }
        free(conf->routing_keys);
        conf->routing_keys = NULL;
    }
    
    return 0;
}

int createDataStoreMsgReceiver(DataStoreConf_t *conf, DataStoreMsgReceiver_t **receiver) {
    int status = 0;
    DataStoreMsgReceiver_t *handle;
    amqp_rpc_reply_t reply;
    amqp_queue_declare_ok_t *queue_status;
    int i;
    
    status = _checkConnConf(conf);
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
    
    handle->thread_run = false;
    
    LOG4CXX_DEBUG(logger, "createDataStoreMsgReceiver: creating a TCP connection to " << conf->hostname);
    
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
    status = amqp_socket_open(handle->socket, conf->hostname, conf->port);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: unable to create a TCP connection to " << conf->hostname);
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }
    
    LOG4CXX_DEBUG(logger, "createDataStoreMsgReceiver: logging in with " << conf->user_id);
    
    // login
    reply = amqp_login(handle->conn_state, "/", 0, AMQP_DEFAULT_FRAME_SIZE, 0, AMQP_SASL_METHOD_PLAIN, conf->user_id, conf->user_pwd);
    if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: unable to login with " << conf->user_id);
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

    if(conf->routing_keys_len == 0) {
        amqp_queue_bind(handle->conn_state, handle->channel, handle->queuename, amqp_cstring_bytes(conf->exchange), amqp_cstring_bytes("#"), amqp_empty_table);
        reply = amqp_get_rpc_reply(handle->conn_state);
        if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
            LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: unable to bind a queue");
            amqp_channel_close(handle->conn_state, handle->channel, AMQP_REPLY_SUCCESS);
            amqp_connection_close(handle->conn_state, AMQP_REPLY_SUCCESS);
            amqp_destroy_connection(handle->conn_state);
            free(handle);
            return EIO;
        }
    } else {
        for(i=0;i<conf->routing_keys_len;i++) {
            amqp_queue_bind(handle->conn_state, handle->channel, handle->queuename, amqp_cstring_bytes(conf->exchange), amqp_cstring_bytes(conf->routing_keys[i]), amqp_empty_table);
            reply = amqp_get_rpc_reply(handle->conn_state);
            if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
                LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver: unable to bind a queue");
                amqp_channel_close(handle->conn_state, handle->channel, AMQP_REPLY_SUCCESS);
                amqp_connection_close(handle->conn_state, AMQP_REPLY_SUCCESS);
                amqp_destroy_connection(handle->conn_state);
                free(handle);
                return EIO;
            }
        }
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
    int i;
    
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