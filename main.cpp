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
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>
#include <log4cxx/helpers/exception.h>
#include "datastore_receiver.hpp"

using namespace std;

/*
 * 
 */

#define LOG_CONFIG_XML "log4cxx.xml"
#define DATASTORE_RECEIVER_CONFIG_JSON "datastore_receiver.json"

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("main"));

int main(int argc, char** argv) {
    log4cxx::xml::DOMConfigurator::configure(LOG_CONFIG_XML);
    
    int status = 0;
    DataStoreConf_t *conf;
    DataStoreMsgReceiver_t *receiver;
    
    LOG4CXX_DEBUG(logger, "iPlant Border Message Server is starting");
    
    status = readDataStoreMsgReceiverConf((char*)DATASTORE_RECEIVER_CONFIG_JSON, &conf);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "readDataStoreMsgReceiverConf failed status = " << status);
        return status;
    }
    
    assert(conf != NULL);
    
    status = createDataStoreMsgReceiver(conf, &receiver);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "createDataStoreMsgReceiver failed status = " << status);
        return status;
    }
    
    assert(receiver != NULL);
    
    LOG4CXX_DEBUG(logger, "Receiving messages");
    status = runDataStoreMsgReceiver(receiver);
    
    sleep(10);
    
    status = releaseDataStoreMsgReceiver(receiver);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "releaseDataStoreMsgReceiver failed status = " << status);
        return status;
    }
    
    releaseDataStoreMsgReceiverConf(conf);
    
    return 0;
}

