/* 
 * File:   datastore_client.cpp
 * Author: iychoi
 *
 * Created on August 27, 2015, 11:00 PM
 */

#include <cstdlib>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <log4cxx/logger.h>
#include <jsoncpp/json/value.h>
#include <jsoncpp/json/reader.h>
#include <iostream>
#include <fstream>
#include <jsoncpp/json/writer.h>
#include "common.hpp"
#include "irodsfslib/libfslib.hpp"
#include "irodsfslib/libfslib.conf.hpp"
#include "irodsfslib/libfslib.operations.hpp"
#include "lrucache.hpp"

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("datastore_client"));

static lru::Cache<string, string> g_uuid_path_cache(1024*1024, 1024);

/*
 * 
 */
int initDataStoreClient(fslibConf_t *conf) {
    int status = 0;
    
    if(conf == NULL) {
        LOG4CXX_ERROR(logger, "initDataStoreClient: conf is null");
        return EINVAL;
    }
    
    status = fslibInit(conf, NULL);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "initDataStoreClient: unable to init data store client");
        return status;
    }
}

int destroyDataStoreClient() {
    return fslibDestroy();
}

int readDataStoreClientConf(char *path, fslibConf_t **conf) {
    fslibConf_t *handle;
    Json::Value confjson;
    Json::Reader reader;
    ifstream istream;
    
    if(path == NULL) {
        LOG4CXX_ERROR(logger, "readDataStoreClientConf: path is null");
        return EINVAL;
    }
    
    if(conf == NULL) {
        LOG4CXX_ERROR(logger, "readDataStoreClientConf: conf is null");
        return EINVAL;
    }
    
    istream.open(path);
    
    bool parsed = reader.parse(istream, confjson, false);
    if(!parsed) {
        LOG4CXX_ERROR(logger, "readDataStoreClientConf: unable to parse configuration file");
        return EINVAL;
    }
    
    *conf = NULL;
    
    handle = (fslibConf_t *)calloc(1, sizeof(fslibConf_t));
    if(handle == NULL) {
        LOG4CXX_ERROR(logger, "readDataStoreClientConf: not enough memory to allocate");
        return ENOMEM;
    }
    
    strcpy(handle->iCAT_host, confjson["hostname"].asCString());
    handle->iCAT_port = confjson["port"].asInt();
    strcpy(handle->user_id, confjson["user_id"].asCString());
    strcpy(handle->user_passwd, confjson["user_pwd"].asCString());
    strcpy(handle->zone, confjson["zone"].asCString());
    
    *conf = handle;
    
    return 0;
}

int releaseDataStoreClientConf(fslibConf_t *conf) {
    if(conf == NULL) {
        LOG4CXX_ERROR(logger, "releaseDataStoreClientConf: conf is null");
        return EINVAL;
    }
    
    free(conf);
    return 0;
}

static bool g_ResultSet = false;
static char g_ResultBufferCollection[MAX_PATH_LEN];
static char g_ResultBufferDataObj[MAX_PATH_LEN];

static int queryResultFiller(const char *name, const char *value) {
    if(strcmp(name, "collection") == 0) {
        strcpy(g_ResultBufferCollection, name);
    } else if(strcmp(name, "dataObj") == 0) {
        strcpy(g_ResultBufferDataObj, value);
    }
    
    g_ResultSet = true;
    
    return 0;
}

int cacheUUIDtoPath(const char *uuid, const char *path) {
    if(uuid == NULL) {
        LOG4CXX_ERROR(logger, "cacheUUIDtoPath: uuid is null");
        return EINVAL;
    }
    
    if(path == NULL) {
        LOG4CXX_ERROR(logger, "cacheUUIDtoPath: path is null");
        return EINVAL;
    }
    
    string uuid_str(uuid);
    string path_str(path);
    
    LOG4CXX_DEBUG(logger, "cacheUUIDtoPath: caching " << uuid_str << " - " << path_str);
    
    g_uuid_path_cache.insert(uuid_str, path_str);
    return 0;
}

int convertUUIDtoPath(const char *uuid, char *buf) {
    int status = 0;
    
    if(uuid == NULL) {
        LOG4CXX_ERROR(logger, "convertUUIDtoPath: uuid is null");
        return EINVAL;
    }
    
    if(buf == NULL) {
        LOG4CXX_ERROR(logger, "convertUUIDtoPath: buf is null");
        return EINVAL;
    }
    
    // check cache first
    string uuid_str(uuid);
    if(g_uuid_path_cache.contains(uuid_str)) {
        strcpy(buf, g_uuid_path_cache.get(uuid_str).c_str());
        return 0;
    }
    
    g_ResultSet = false;
    
    status = fslibQueryDataObject("ipc_UUID", "=", uuid, queryResultFiller);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "convertUUIDtoPath: fslibQueryDataObject failed status = " << status);
        return status;
    }
    
    if(g_ResultSet) {
        // copy to buf
        sprintf(buf, "%s/%s", g_ResultBufferCollection, g_ResultBufferDataObj);
        
        cacheUUIDtoPath(uuid, buf);
        
        // clear
        memset(g_ResultBufferCollection, 0, MAX_PATH_LEN);
        memset(g_ResultBufferDataObj, 0, MAX_PATH_LEN);
        g_ResultSet = false;
        return 0;
    }
    
    return ENOENT;
}