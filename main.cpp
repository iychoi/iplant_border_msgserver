/* 
 * File:   main.cpp
 * Author: iychoi
 *
 * Created on September 17, 2015, 3:47 PM
 */

#include <cstdlib>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <signal.h>
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>
#include <log4cxx/helpers/exception.h>
#include "common.hpp"
#include "publisher.hpp"
#include "datastore_receiver.hpp"

using namespace std;

/*
 * 
 */

#define LOG_CONFIG_XML "log4cxx.xml"
#define PUBLISHER_CONFIG_JSON "publisher.json"
#define DATASTORE_RECEIVER_CONFIG_JSON "datastore_receiver.json"

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("main"));

static bool g_ContinueEventLoop;

static void interruptHandler(int sig) {
    g_ContinueEventLoop = false;
}

static void eventLoop() {
    signal(SIGINT, interruptHandler);
    
    LOG4CXX_DEBUG(logger, "EventLoop Start");
    
    g_ContinueEventLoop = true;
    while(g_ContinueEventLoop) {
        sleep(1);
    }
    
    LOG4CXX_DEBUG(logger, "EventLoop End");
}

int main(int argc, char** argv) {
    log4cxx::xml::DOMConfigurator::configure(LOG_CONFIG_XML);
    
    int status = 0;
    PublisherConf_t *publisher_conf;
    Publisher_t *publisher;
    DataStoreConf_t *receiver_conf;
    DataStoreMsgReceiver_t *receiver;
    
    LOG4CXX_DEBUG(logger, "iPlant Border Message Server is starting");
    
    status = readPublisherConf((char*)PUBLISHER_CONFIG_JSON, &publisher_conf);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "readPublisherConf failed status = " << status);
        return status;
    }
    
    assert(receiver_conf != NULL);
    
    status = createPublisher(publisher_conf, &publisher);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "createPublisher failed status = " << status);
        return status;
    }
    
    assert(publisher != NULL);
    
    status = readDataStoreMsgReceiverConf((char*)DATASTORE_RECEIVER_CONFIG_JSON, &receiver_conf);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "readDataStoreMsgReceiverConf failed status = " << status);
        return status;
    }
    
    assert(receiver_conf != NULL);
    
    status = createDataStoreMsgReceiver(receiver_conf, publisher, &receiver);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver failed status = " << status);
        return status;
    }
    
    assert(receiver != NULL);
    
    LOG4CXX_DEBUG(logger, "Receiving messages");
    status = runDataStoreMsgReceiver(receiver);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "runDataStoreMsgReceiver failed status = " << status);
        return status;
    }
    
    eventLoop();
    
    status = releaseDataStoreMsgReceiver(receiver);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "releaseDataStoreMsgReceiver failed status = " << status);
        return status;
    }
    
    releaseDataStoreMsgReceiverConf(receiver_conf);
    
    status = releasePublisher(publisher);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "releasePublisher failed status = " << status);
        return status;
    }
    
    releasePublisherConf(publisher_conf);
    return 0;
}

