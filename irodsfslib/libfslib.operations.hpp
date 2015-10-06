/*** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/
/*** This code is rewritten by Illyoung Choi (iychoi@email.arizona.edu)    ***
 *** funded by iPlantCollaborative (www.iplantcollaborative.org).          ***/
#ifndef FSLIB_OPERATIONS_HPP
#define FSLIB_OPERATIONS_HPP

#include <sys/statvfs.h>

typedef struct _fslibFileInfo {
    int flags;
    unsigned long int fh;
} fslibFileInfo_t;

typedef int(* fslibFillDir_t)(void *buf, const char *name, const struct stat *stbuf, off_t off);
typedef int(* fslibFillQueryResult_t) (const char *name, const char *value);

#ifdef  __cplusplus
extern "C" {
#endif
    void fslibOperationsInit();
    void fslibOperationsDestroy();
    
    int fslibGetAttr(const char *path, struct stat *stbuf);
    int fslibOpen(const char *path, fslibFileInfo_t *fi);
    int fslibClose(fslibFileInfo_t *fi);
    int fslibFlush(fslibFileInfo_t *fi);
    int fslibRead(char *buf, size_t size, off_t offset, fslibFileInfo_t *fi);
    int fslibWrite(const char *buf, size_t size, off_t offset, fslibFileInfo_t *fi);
    int fslibCreate(const char *path, mode_t mode, dev_t rdev);
    int fslibUnlink(const char *path);
    int fslibLink(const char *from, const char *to);
    int fslibStatfs(const char *path, struct statvfs *stbuf);
    int fslibOpenDir(const char *path, fslibFileInfo_t *fi);
    int fslibCloseDir(fslibFileInfo_t *fi);
    int fslibReadDir(void *buf, fslibFillDir_t filler, off_t offset, fslibFileInfo_t *fi);
    int fslibMakeDir(const char *path, mode_t mode);
    int fslibRemoveDir(const char *path);
    int fslibRename(const char *from, const char *to);
    int fslibTruncate(const char *path, off_t size);
    int fslibSymlink(const char *from, const char *to);
    int fslibReadLink(const char *path, char *buf, size_t size);
    int fslibChmod(const char *path, mode_t mode);
    int fslibQueryDataObject(const char *key, const char *operation, const char *value, fslibFillQueryResult_t filler);
    int fslibQueryCollection(const char *key, const char *operation, const char *value, fslibFillQueryResult_t filler);
#ifdef  __cplusplus
}
#endif

#endif	/* FSLIB_OPERATIONS_HPP */
