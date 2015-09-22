/* 
 * File:   msgbuffer.hpp
 * Author: iychoi
 *
 * Created on September 17, 2015, 9:54 PM
 */

#ifndef MSGBUFFER_HPP
#define	MSGBUFFER_HPP

#include <stdint.h>

typedef struct _GenericMsg {
    char *exchange;
    int exchange_len;
    char *routing_key;
    int routing_key_len;
    char *queuename;
    int queuename_len;
    char *binding;
    int binding_len;
    char *body;
    int body_len;
} GenericMsg_t;

int initMsgBuffer();
int destroyMsgBuffer();
int createGenericMessage(char *exchange, char *routing_key, char *queuename, char *binding, char *body, GenericMsg_t **genericMsg);
int createGenericMessage(char *exchange, int exchange_len, char *routing_key, int routing_key_len, char *queuename, int queuename_len, char *binding, int binding_len, char *body, int body_len, GenericMsg_t **genericMsg);
int releaseGenericMessage(GenericMsg_t *msg);
int putMessage(GenericMsg_t *msg);
GenericMsg_t * getMessage();


#endif	/* MSGBUFFER_HPP */

