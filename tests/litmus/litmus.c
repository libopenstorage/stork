#define _GNU_SOURCE

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>

int main(int argc, char **argv)
{
        int fd;

        if (argc != 2) {
                fprintf(stderr, "Usage %s <file>\n", argv[0]);
                return 1;
        }

        fd = open(argv[1], O_WRONLY | O_CREAT);
        if (fd < 0)
                return 1;

        write(fd, "Hello World\n", 12);

        /* sync data to disk */
        syncfs(fd);

        /* cause a kernel panic */
        system("echo c > /proc/sysrq-trigger");

        if (close(fd) < 0)
                return 1;

        return 0;
}
