/*** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/
/*** This code is rewritten by Illyoung Choi (iychoi@email.arizona.edu)    ***
 *** funded by iPlantCollaborative (www.iplantcollaborative.org).          ***/
#ifndef FSLIB_CONF_HPP
#define FSLIB_CONF_HPP

#include <sys/statvfs.h>

typedef struct _fslibConf {
    char iCAT_host[1024];
    int iCAT_port;
    char zone[100];
    char user_id[100];
    char user_passwd[100];
    char home[1024];
    char cwd[1024];
} fslibConf_t;

typedef struct _fslibOpt {
    bool bufferedFS;
    bool preload;
    int maxConn;
    int connTimeoutSec;
    int connKeepAliveSec;
} fslibOpt_t;

#ifdef  __cplusplus
extern "C" {
#endif
    fslibConf_t *fslibGetConf();
    void fslibSetConf(fslibConf_t *pConf);
    fslibOpt_t *fslibGetOption();
    void fslibSetOption(fslibOpt_t *pOpt);
#ifdef  __cplusplus
}
#endif

#endif	/* FSLIB_CONF_HPP */
