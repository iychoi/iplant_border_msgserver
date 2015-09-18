/* 
 * File:   receiver.hpp
 * Author: iychoi
 *
 * Created on September 17, 2015, 9:54 PM
 */

#ifndef RECEIVER_HPP
#define	RECEIVER_HPP

#include <stdint.h>

typedef struct _GenericMsg {
    uint64_t delivery_tag;
    char *exchange;
    int exchange_len;
    char *routing_key;
    int routing_key_len;
    char *body;
    int body_len;
} GenericMsg_t;

int receive(GenericMsg_t *msg);


#endif	/* RECEIVER_HPP */

