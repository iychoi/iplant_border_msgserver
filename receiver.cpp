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
#include <log4cxx/logger.h>
#include "receiver.hpp"
#include <jsoncpp/json/json.h>

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("receiver"));

/*
 * Receive messages from source and send to processor
 */
void freeGenericMessage(GenericMsg_t *msg) {
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
        freeGenericMessage(msg);
        return EINVAL;
    }
    
    Json::Value author = msgjson["author"];
    Json::Value entity = msgjson["entity"];
    Json::Value metadatum = msgjson["metadatum"];
    
    printf("AUTHOR: %s\n", author.toStyledString().c_str());
    
    
    freeGenericMessage(msg);
    return 0;
}