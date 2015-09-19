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
#include "receiver.hpp"
#include <jsoncpp/json/json.h>

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("receiver"));

/*
 * Receive messages from source and send to processor
 */
int createGenericMessage(amqp_envelope_t *envelope, GenericMsg_t **genericMsg) {
    GenericMsg_t *gmsg;
    
    if(envelope == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: envelope is null");
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

    gmsg->exchange = (char *)calloc(envelope->exchange.len + 1, 1);
    gmsg->routing_key = (char *)calloc(envelope->routing_key.len + 1, 1);
    gmsg->body = (char *)calloc(envelope->message.body.len + 1, 1);
    if(gmsg->exchange == NULL || gmsg->routing_key == NULL || gmsg->body == NULL) {
        LOG4CXX_ERROR(logger, "createGenericMessage: not enough memory to allocate");
        return ENOMEM;
    }

    gmsg->delivery_tag = envelope->delivery_tag;
    gmsg->exchange_len = envelope->exchange.len;
    memcpy(gmsg->exchange, (char *)envelope->exchange.bytes, envelope->exchange.len);
    gmsg->routing_key_len = envelope->routing_key.len;
    memcpy(gmsg->routing_key, (char *)envelope->routing_key.bytes, envelope->routing_key.len);
    gmsg->body_len = envelope->message.body.len;
    memcpy(gmsg->body, (char *)envelope->message.body.bytes, envelope->message.body.len);
    
    *genericMsg = gmsg;
    return 0;
}

int releaseGenericMessage(GenericMsg_t *msg) {
    if(msg == NULL) {
        LOG4CXX_ERROR(logger, "freeGenericMessage: msg is null");
    }
    
    if(msg->exchange != NULL) {
        free(msg->exchange);
        msg->exchange = NULL;
    }
    
    if(msg->routing_key != NULL) {
        free(msg->routing_key);
        msg->routing_key = NULL;
    }
    
    if(msg->body != NULL) {
        free(msg->body);
        msg->body = NULL;
    }
    
    free(msg);
    return 0;
}

int receive(GenericMsg_t *msg) {
    Json::Value msgjson;
    Json::Reader reader;
    
    if(msg == NULL) {
        LOG4CXX_ERROR(logger, "receive: msg is null");
        return EINVAL;
    }
    
    LOG4CXX_DEBUG(logger, "receive: " << msg->delivery_tag << ":" << msg->exchange << ":" << msg->routing_key << "\t" << msg->body);
    
    bool parsed = reader.parse(msg->body, msgjson, false);
    if(!parsed) {
        LOG4CXX_ERROR(logger, "receive: unable to parse message body");
        releaseGenericMessage(msg);
        return EINVAL;
    }
    
    Json::Value author = msgjson["author"];
    Json::Value entity = msgjson["entity"];
    Json::Value metadatum = msgjson["metadatum"];
    
    printf("AUTHOR: %s\n", author.toStyledString().c_str());
    
    
    releaseGenericMessage(msg);
    return 0;
}