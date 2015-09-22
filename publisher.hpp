/* 
 * File:   publisher.hpp
 * Author: iychoi
 *
 * Created on September 18, 2015, 9:53 PM
 */

#ifndef PUBLISHER_HPP
#define	PUBLISHER_HPP

#include <pthread.h>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#include "common.hpp"

typedef struct _PublisherConf {
    char hostname[HOSTNAME_MAX_LEN];
    int port;
    char user_id[CREDENTIAL_MAX_LEN];
    char user_pwd[CREDENTIAL_MAX_LEN];
} PublisherConf_t;

typedef struct _Publisher {
    pthread_t thread;
    bool thread_run;
    amqp_socket_t *socket;
    amqp_connection_state_t conn_state;
    amqp_channel_t channel;
    amqp_bytes_t queuename;
} Publisher_t;

int readPublisherConf(char *path, PublisherConf_t **conf);
int releasePublisherConf(PublisherConf_t *conf);
int createPublisher(PublisherConf_t *conf, Publisher_t **publisher);
int releasePublisher(Publisher_t *publisher);
int runPublisher(Publisher_t *publisher);

#endif	/* PUBLISHER_HPP */

