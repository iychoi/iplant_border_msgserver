#ifndef LIBIRODSFS_HPP
#define LIBIRODSFS_HPP

#include <sys/statvfs.h>

typedef struct _irodsfsFileInfo {
    int flags;
    unsigned long int fh;
} irodsfsFileInfo_t;

typedef int(* irodsfsFillDir_t)(void *buf, const char *name, const struct stat *stbuf, off_t off);
typedef int(* irodsfsFillQueryResult_t) (const char *name, const char *value);

typedef struct _irodsfsConf {
    char iCAT_host[1024];
    int iCAT_port;
    char zone[100];
    char user_id[100];
    char user_passwd[100];
    char home[1024];
    char cwd[1024];
} irodsfsConf_t;

typedef struct _irodsfsOpt {
    bool bufferedFS;
    bool preload;
    int maxConn;
    int connTimeoutSec;
    int connKeepAliveSec;
} irodsfsOpt_t;

#ifdef  __cplusplus
extern "C" {
#endif
    irodsfsConf_t *irodsfsGetConf();
    void irodsfsSetConf(irodsfsConf_t *pConf);
    irodsfsOpt_t *irodsfsGetOption();
    void irodsfsSetOption(irodsfsOpt_t *pOpt);
    
    int irodsfsInit(irodsfsConf_t *conf, irodsfsOpt_t *opt);
    int irodsfsDestroy();
    
    int irodsfsGetAttr(const char *path, struct stat *stbuf);
    int irodsfsOpen(const char *path, int flags, irodsfsFileInfo_t *fi);
    int irodsfsClose(irodsfsFileInfo_t *fi);
    int irodsfsFlush(irodsfsFileInfo_t *fi);
    int irodsfsRead(char *buf, size_t size, off_t offset, irodsfsFileInfo_t *fi);
    int irodsfsWrite(const char *buf, size_t size, off_t offset, irodsfsFileInfo_t *fi);
    int irodsfsCreate(const char *path, mode_t mode);
    int irodsfsUnlink(const char *path);
    int irodsfsLink(const char *from, const char *to);
    int irodsfsStatfs(const char *path, struct statvfs *stbuf);
    int irodsfsOpenDir(const char *path, irodsfsFileInfo_t *fi);
    int irodsfsCloseDir(irodsfsFileInfo_t *fi);
    int irodsfsReadDir(void *buf, irodsfsFillDir_t filler, off_t offset, irodsfsFileInfo_t *fi);
    int irodsfsMakeDir(const char *path, mode_t mode);
    int irodsfsRemoveDir(const char *path);
    int irodsfsRename(const char *from, const char *to);
    int irodsfsTruncate(const char *path, off_t size);
    int irodsfsSymlink(const char *from, const char *to);
    int irodsfsReadLink(const char *path, char *buf, size_t size);
    int irodsfsChmod(const char *path, mode_t mode);
    int irodsfsQueryDataObject(const char *key, const char *operation, const char *value, irodsfsFillQueryResult_t filler);
    int irodsfsQueryCollection(const char *key, const char *operation, const char *value, irodsfsFillQueryResult_t filler);
#ifdef  __cplusplus
}
#endif

#endif	/* LIBIRODSFS_HPP */
