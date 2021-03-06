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
#include <jsoncpp/json/value.h>
#include <jsoncpp/json/reader.h>
#include <iostream>
#include <fstream>
#include <pthread.h>
#include "common.hpp"
#include "publisher.hpp"

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("publisher"));

static int _checkConnConf(PublisherConf_t *conn) {
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
    
    if(strlen(conn->vhost) == 0) {
        LOG4CXX_ERROR(logger, "_checkConnConf: conn.vhost is empty");
        return EINVAL;
    }
    
    return 0;
}

int readPublisherConf(char *path, PublisherConf_t **conf) {
    PublisherConf_t *handle;
    Json::Value confjson;
    Json::Reader reader;
    ifstream istream;
    
    if(path == NULL) {
        LOG4CXX_ERROR(logger, "readPublisherConf: path is null");
        return EINVAL;
    }
    
    if(conf == NULL) {
        LOG4CXX_ERROR(logger, "readPublisherConf: conf is null");
        return EINVAL;
    }
    
    istream.open(path);
    
    bool parsed = reader.parse(istream, confjson, false);
    if(!parsed) {
        LOG4CXX_ERROR(logger, "readPublisherConf: unable to parse configuration file");
        return EINVAL;
    }
    
    *conf = NULL;
    
    handle = (PublisherConf_t *)calloc(1, sizeof(PublisherConf_t));
    if(handle == NULL) {
        LOG4CXX_ERROR(logger, "readPublisherConf: not enough memory to allocate");
        return ENOMEM;
    }
    
    strcpy(handle->hostname, confjson["hostname"].asCString());
    handle->port = confjson["port"].asInt();
    strcpy(handle->user_id, confjson["user_id"].asCString());
    strcpy(handle->user_pwd, confjson["user_pwd"].asCString());
    strcpy(handle->vhost, confjson["vhost"].asCString());
    
    *conf = handle;
    
    return 0;
}

int releasePublisherConf(PublisherConf_t *conf) {
    if(conf == NULL) {
        LOG4CXX_ERROR(logger, "releasePublisherConf: conf is null");
        return EINVAL;
    }
    
    free(conf);
    return 0;
}

int createPublisher(PublisherConf_t *conf, Publisher_t **publisher) {
    int status = 0;
    Publisher_t *handle;
    amqp_rpc_reply_t reply;
    
    status = _checkConnConf(conf);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "createPublisher: connection configuration check failed");
        return status;
    }
    
    if(publisher == NULL) {
        LOG4CXX_ERROR(logger, "createPublisher: publisher is null");
        return EINVAL;
    }
    
    *publisher = NULL;
    
    handle = new Publisher_t();
    if(handle == NULL) {
        LOG4CXX_ERROR(logger, "createPublisher: not enough memory to allocate");
        return ENOMEM;
    }
    
    LOG4CXX_DEBUG(logger, "createPublisher: creating a TCP connection to " << conf->hostname);
    
    // create a TCP connection
    handle->conn_state = amqp_new_connection();
    handle->socket = amqp_tcp_socket_new(handle->conn_state);
    if(handle->socket == NULL) {
        LOG4CXX_ERROR(logger, "createPublisher: unable to create a connection");
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }
    
    // open a socket
    status = amqp_socket_open(handle->socket, conf->hostname, conf->port);
    if(status != 0) {
        LOG4CXX_ERROR(logger, "createPublisher: unable to create a TCP connection to " << conf->hostname);
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }
    
    LOG4CXX_DEBUG(logger, "createPublisher: logging in with " << conf->user_id);
    
    // login
    reply = amqp_login(handle->conn_state, conf->vhost, 0, AMQP_DEFAULT_FRAME_SIZE, 0, AMQP_SASL_METHOD_PLAIN, conf->user_id, conf->user_pwd);
    if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
        LOG4CXX_ERROR(logger, "createPublisher: unable to login with " << conf->user_id);
        amqp_connection_close(handle->conn_state, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }
    
    LOG4CXX_DEBUG(logger, "createPublisher: opening a channel");
    
    // open a channel
    handle->channel = 1;
    amqp_channel_open(handle->conn_state, handle->channel);
    reply = amqp_get_rpc_reply(handle->conn_state);
    if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
        LOG4CXX_ERROR(logger, "createPublisher: unable to open a channel");
        amqp_connection_close(handle->conn_state, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(handle->conn_state);
        free(handle);
        return EIO;
    }
    
    *publisher = handle;
    return 0;
}

int releasePublisher(Publisher_t *publisher) {
    if(publisher == NULL) {
        LOG4CXX_ERROR(logger, "releasePublisher: publisher is null");
        return EINVAL;
    }
    
    amqp_channel_close(publisher->conn_state, publisher->channel, AMQP_REPLY_SUCCESS);
    amqp_connection_close(publisher->conn_state, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(publisher->conn_state);
    
    delete publisher;
    LOG4CXX_DEBUG(logger, "releasePublisher: closed connection");
    return 0;
}

int publish(Publisher_t *publisher, const char *exchange, const char *routing_key, const char *body) {
    int status = 0;
    amqp_rpc_reply_t reply;
    std::map<std::string, std::string>::iterator it;
    std::string key(exchange);
    
    if(publisher == NULL) {
        LOG4CXX_ERROR(logger, "publish: publisher is null");
        return EINVAL;
    }
    
    if(!key.empty()) {
        it = publisher->exchanges.find(key);
        if(it == publisher->exchanges.end()) {
            amqp_exchange_declare_ok_t *exchange_status;

            LOG4CXX_DEBUG(logger, "publish: declaring an exchange : " << key);

            // declare an exchange
            exchange_status = amqp_exchange_declare(publisher->conn_state, publisher->channel, amqp_cstring_bytes(exchange), amqp_cstring_bytes("direct"), 0, 0, 0, 0, amqp_empty_table);
            reply = amqp_get_rpc_reply(publisher->conn_state);
            if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
                LOG4CXX_ERROR(logger, "publish: unable to declare an exchange");
            } else {
                publisher->exchanges[key] = key;
            }

            /*
            LOG4CXX_DEBUG(logger, "_publisherThread: declaring and binding a queue");

            // declare a queue
            queue_status = amqp_queue_declare(publisher->conn_state, publisher->channel, amqp_cstring_bytes(msg->queuename), 0, 0, 1, 1, amqp_empty_table);
            reply = amqp_get_rpc_reply(publisher->conn_state);
            if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
                LOG4CXX_ERROR(logger, "_publisherThread: unable to declare a queue");
            } else {
                // bind a queue
                amqp_queue_bind(publisher->conn_state, publisher->channel, amqp_cstring_bytes(msg->queuename), amqp_cstring_bytes(msg->exchange), amqp_cstring_bytes(msg->binding), amqp_empty_table);
                reply = amqp_get_rpc_reply(publisher->conn_state);
                if(reply.reply_type != AMQP_RESPONSE_NORMAL) {
                    LOG4CXX_ERROR(logger, "_publisherThread: unable to bind a queue");
                } else {
                    publisher->queuenames[key] = key;
                }
            }
            */
        }
    }
    
    if(key.empty()) {
        LOG4CXX_DEBUG(logger, "publish: DEFAULT:" << routing_key << "\t" << body);
    } else {
        LOG4CXX_DEBUG(logger, "publish: " << exchange << ":" << routing_key << "\t" << body);
    }
    
    // send
    status = amqp_basic_publish(publisher->conn_state, publisher->channel, amqp_cstring_bytes(exchange), amqp_cstring_bytes(routing_key), 0, 0, NULL, amqp_cstring_bytes(body));
    if(status != 0) {
        LOG4CXX_ERROR(logger, "publish: unable to send a message - status " << status << " err " << amqp_error_string2(status));
    }
    
    return 0;
}