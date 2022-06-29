#define _GNU_SOURCE

#include <unistd.h>
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/vfs.h> /* or <sys/statfs.h> */
#include <sys/statvfs.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>

/*
 * To compile:
 *  gcc -g -shared -fPIC -o px_statfs.so px_statfs.c -ldl -D__USE_LARGEFILE64
 */

typedef int (*real_statfs_t)(const char *path, struct statfs *buf);
extern int lstat(const char *pathname, struct stat *statbuf);

ssize_t real_statfs(const char *path, struct statfs *buf)
{
    return ((real_statfs_t)dlsym(RTLD_NEXT, "statfs"))(path, buf);
}

void read_to_str(char *filename, char *buf, int read_cnt)
{
    int fd = -1;
    fd = open(filename, O_RDONLY);
    if (fd <= 0)
    {
        return;
    }
    read(fd, buf, read_cnt);
    close(fd);
    return;
}

/*
 * For regular pxd devices
 * check /sys/devices/pxd/1/major
 */
int is_pxd_reg_device(int major, int minor)
{
    char filename[256] = {0};
    char major_nr_str[10] = {0};
    sprintf(filename, "/sys/devices/pxd/%d/major", minor);
    read_to_str(filename, major_nr_str, 9);
    if (strlen(major_nr_str) > 0)
    {
        int pxd_major = atoi(major_nr_str);
        if (pxd_major == major)
        {
            return 1;
        }
    }
    return 0;
}

/*
 * For encrypted device
 * # cat /sys/dev/block/253:0/dm/uuid
 *    CRYPT-LUKS1-41d2686ad84f43d2a64f20502c2f9db0-pxd-enc706349485201319665
 * # cat /sys/dev/block/253:0/dm/name
 *    pxd-enc706349485201319665
 */
int is_pxd_enc_device(int major, int minor)
{
    char filename[256] = {0};
    char pxd_namestr[10] = {0};
    char *pattern = "pxd-enc\0";
    sprintf(filename, "/sys/dev/block/%d:%d/dm/name", major, minor);
    read_to_str(filename, pxd_namestr, 9);
    if (strlen(pxd_namestr) > 0)
    {
        if (strncmp(pxd_namestr, pattern, strlen(pattern)) == 0)
        {
            return 1;
        }
    }
    return 0;
}

/*
 * For loopback devices
 * # cat /sys/dev/block/7:0/loop/backing_file
 *    /dev/pxd/pxd7871666956677911
 */
int is_pxd_passthrough_device(int major, int minor)
{
    char filename[256] = {0};
    char pxd_namestr[20] = {0};
    char *pattern = "/dev/pxd/pxd\0";
    sprintf(filename, "/sys/dev/block/%d:%d/loop/backing_file", major, minor);
    read_to_str(filename, pxd_namestr, 19);
    if (strlen(pxd_namestr) > 0)
    {
        if (strncmp(pxd_namestr, pattern, strlen(pattern)) == 0)
        {
            return 1;
        }
    }
    return 0;
}

int is_px_device(int major, int minor)
{
    int is_true;

    is_true = is_pxd_reg_device(major, minor);
    if (is_true)
        return 1;

    is_true = is_pxd_enc_device(major, minor);
    if (is_true)
        return 1;

    is_true = is_pxd_passthrough_device(major, minor);
    if (is_true)
        return 1;

    return 0;
}

int get_major_minor_nr(const char *path, int *major, int *minor)
{
    struct stat statbuf = {0};
    int rc = 0;
    rc = lstat(path, &statbuf);
    if (rc != 0)
    {
        *major = 0;
        *minor = 0;
        return -1;
    }
    unsigned int maj = (unsigned int)((statbuf.st_dev & 0x000000000000ff00) >> 8);
    unsigned int min = (unsigned int)((statbuf.st_dev & 0x00000000000000ff));
    *major = maj;
    *minor = min;
    return 0;
}

int statfs(const char *path, struct statfs *buf)
{
    int rc = -1;

    // Perform the actual system call
    rc = real_statfs(path, buf);

    if (rc == 0)
    {
        // TODO: do we need to check buf->f_type in (ext4 | XFS) ?
        int major = 0;
        int minor = 0;
        int rc2 = get_major_minor_nr(path, &major, &minor);
        if (rc2 != 0)
        {
            return rc;
        }

        int is_pxd = is_px_device(major, minor);
        if (is_pxd == 1)
        {
            buf->f_type = 0x6969; // NFS_SUPER_MAGIC
        }
    }

    // Behave just like the regular syscall would
    return rc;
}

int __statfs(const char *path, struct statfs *buf)
{
    return statfs(path, buf);
}

int statfs64(const char *path, struct statfs64 *buf)
{
    return statfs(path, (struct statfs *)buf);
}
