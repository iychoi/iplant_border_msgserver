/* 
 * File:   publisher.cpp
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
#include <pthread.h>
#include "msgbuffer.hpp"

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("publisher"));

static pthread_t g_thread;

/*
 * Receive messages from source and send to processor
 */
static void* _publisherThread(void* param) {
    int status = 0;
    
    LOG4CXX_DEBUG(logger, "_publisherThread: publisher thread started");
    
    while(1) {
        GenericMsg_t *msg = NULL;
    
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
        
        msg = getMessage();
        if(msg != NULL) {
            LOG4CXX_DEBUG(logger, "_publisherThread: " << msg->exchange << ":" << msg->routing_key << "\t" << msg->body);
            releaseGenericMessage(msg);
        }
        
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    }
    
    LOG4CXX_DEBUG(logger, "_publisherThread: publisher thread terminated for unknown reason");
}


int initPublisher() {
    int status;
    
    // create a thread
    status = pthread_create(&g_thread, NULL, _publisherThread, NULL);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "initPublisher: unable to create a publisher thread");
        return status;
    }
    
    return 0;
}

int destroyPublisher() {
    // stop thread
    LOG4CXX_DEBUG(logger, "destroyPublisher: canceling a publisher thread");
    pthread_cancel(g_thread);
    
    return 0;
}
