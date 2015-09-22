/* 
 * File:   receiver.cpp
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
#include <queue>
#include <pthread.h>
#include "msgbuffer.hpp"

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("msgbuffer"));

static std::queue<GenericMsg_t*> g_MsgQueue;
static pthread_mutex_t g_MsgQueueLock;
static pthread_mutexattr_t g_MsgQueueLockAttr;

/*
 * Receive messages from source and send to processor
 */
int initMsgBuffer() {
    pthread_mutexattr_init(&g_MsgQueueLockAttr);
    pthread_mutexattr_settype(&g_MsgQueueLockAttr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&g_MsgQueueLock, &g_MsgQueueLockAttr);
    
    return 0;
}

int destroyMsgBuffer() {
    pthread_mutex_destroy(&g_MsgQueueLock);
    pthread_mutexattr_destroy(&g_MsgQueueLockAttr);
    
    return 0;
}

int createGenericMessage(char *exchange, char *routing_key, char *queuename, char *binding, char *body, GenericMsg_t **genericMsg) {
    int exchange_len;
    int routing_key_len;
    int queuename_len;
    int binding_len;
    int body_len;
    
    if(exchange == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: exchange is null");
        return EINVAL;
    }
    
    if(routing_key == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: routing_key is null");
        return EINVAL;
    }
    
    if(queuename == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: queuename is null");
        return EINVAL;
    }
    
    if(binding == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: binding is null");
        return EINVAL;
    }
    
    if(body == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: body is null");
        return EINVAL;
    }
    
    if(genericMsg == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: genericMsg is null");
        return EINVAL;
    }
    
    exchange_len = strlen(exchange);
    routing_key_len = strlen(routing_key);
    queuename_len = strlen(queuename);
    binding_len = strlen(binding);
    body_len = strlen(body);
    
    return createGenericMessage(exchange, exchange_len, routing_key, routing_key_len, queuename, queuename_len, binding, binding_len, body, body_len, genericMsg);
}

int createGenericMessage(char *exchange, int exchange_len, char *routing_key, int routing_key_len, char *queuename, int queuename_len, char *binding, int binding_len, char *body, int body_len, GenericMsg_t **genericMsg) {
    GenericMsg_t *gmsg;
    
    if(exchange == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: exchange is null");
        return EINVAL;
    }
    
    if(routing_key == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: routing_key is null");
        return EINVAL;
    }
    
    if(queuename == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: queuename is null");
        return EINVAL;
    }
    
    if(binding == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: binding is null");
        return EINVAL;
    }
    
    if(body == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: body is null");
        return EINVAL;
    }
    
    if(genericMsg == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: genericMsg is null");
        return EINVAL;
    }
    
    *genericMsg = NULL;
    
    gmsg = (GenericMsg_t *)calloc(1, sizeof(GenericMsg_t));
    if(gmsg == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: not enough memory to allocate");
        return ENOMEM;
    }

    gmsg->exchange = (char *)calloc(exchange_len + 1, 1);
    gmsg->routing_key = (char *)calloc(routing_key_len + 1, 1);
    gmsg->queuename = (char *)calloc(queuename_len + 1, 1);
    gmsg->binding = (char *)calloc(binding_len + 1, 1);
    gmsg->body = (char *)calloc(body_len + 1, 1);
    if(gmsg->exchange == NULL || gmsg->routing_key == NULL || gmsg->queuename == NULL || gmsg->binding == NULL || gmsg->body == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: not enough memory to allocate");
        return ENOMEM;
    }

    gmsg->exchange_len = exchange_len;
    memcpy(gmsg->exchange, exchange, exchange_len);
    gmsg->routing_key_len = routing_key_len;
    memcpy(gmsg->routing_key, routing_key, routing_key_len);
    gmsg->queuename_len = queuename_len;
    memcpy(gmsg->queuename, queuename, queuename_len);
    gmsg->binding_len = binding_len;
    memcpy(gmsg->binding, binding, binding_len);
    gmsg->body_len = body_len;
    memcpy(gmsg->body, body, body_len);
    
    *genericMsg = gmsg;
    return 0;
}

int releaseGenericMessage(GenericMsg_t *msg) {
    if(msg == NULL) {
        LOG4CXX_ERROR(logger, "releaseGenericMessage: msg is null");
    }
    
    if(msg->exchange != NULL) {
        free(msg->exchange);
        msg->exchange = NULL;
    }
    
    if(msg->routing_key != NULL) {
        free(msg->routing_key);
        msg->routing_key = NULL;
    }
    
    if(msg->queuename != NULL) {
        free(msg->queuename);
        msg->queuename = NULL;
    }
    
    if(msg->binding != NULL) {
        free(msg->binding);
        msg->binding = NULL;
    }
    
    if(msg->body != NULL) {
        free(msg->body);
        msg->body = NULL;
    }
    
    free(msg);
    return 0;
}

int putMessage(GenericMsg_t *msg) {
    if(msg == NULL) {
        LOG4CXX_ERROR(logger, "putMessage: msg is null");
        return EINVAL;
    }
    
    pthread_mutex_lock(&g_MsgQueueLock);
    g_MsgQueue.push(msg);
    pthread_mutex_unlock(&g_MsgQueueLock);
    
    LOG4CXX_DEBUG(logger, "putMessage: " << msg->routing_key);
    
    return 0;
}

GenericMsg_t * getMessage() {
    GenericMsg_t *msg = NULL;
    
    pthread_mutex_lock(&g_MsgQueueLock);
    if(g_MsgQueue.empty()) {
        msg = NULL;
    } else {
        msg = g_MsgQueue.front();
        g_MsgQueue.pop();
    }
    pthread_mutex_unlock(&g_MsgQueueLock);

    return msg;
}