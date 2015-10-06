/*** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/
/*** This code is rewritten by Illyoung Choi (iychoi@email.arizona.edu)    ***
 *** funded by iPlantCollaborative (www.iplantcollaborative.org).          ***/
#ifndef FSLIB_HPP
#define FSLIB_HPP

#include <sys/statvfs.h>
#include "libfslib.conf.hpp"
#include "libfslib.operations.hpp"

#ifdef  __cplusplus
extern "C" {
#endif
    int fslibInit(fslibConf_t *conf, fslibOpt_t *opt);
    int fslibDestroy();    
#ifdef  __cplusplus
}
#endif

#endif	/* FSLIB_HPP */
