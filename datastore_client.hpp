/* 
 * File:   datastore_client.hpp
 * Author: iychoi
 *
 * Created on October 5, 2015, 8:36 PM
 */

#ifndef DATASTORE_CLIENT_HPP
#define	DATASTORE_CLIENT_HPP

#include "common.hpp"
#include "irodsfslib/libfslib.hpp"
#include "irodsfslib/libfslib.conf.hpp"
#include "irodsfslib/libfslib.operations.hpp"

int initDataStoreClient(fslibConf_t *conf);
int destroyDataStoreClient();
int readDataStoreClientConf(char *path, fslibConf_t **conf);
int releaseDataStoreClientConf(fslibConf_t *conf);

int cacheUUIDtoPath(const char *uuid, const char *path);
int convertUUIDtoPath(const char *uuid, char *buf);

#endif	/* DATASTORE_CLIENT_HPP */

