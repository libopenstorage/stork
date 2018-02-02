#define _GNU_SOURCE

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>

void usage(char *prog)
{
	fprintf(stderr, "Usage %s [run|verify] <file>\n", prog);
	exit(1);
}

int main(int argc, char **argv)
{
	int fd;
	int run_or_verify = 0;

	if (argc != 3) {
		usage(argv[0]);
	}

	if (!strcmp(argv[1], "run")) {
		run_or_verify = 1;
	} else if (!strcmp(argv[1], "verify")) {
		run_or_verify = 0;
	} else {
		usage(argv[0]);
	}

	if (run_or_verify) {
		fd = open(argv[2], O_WRONLY | O_CREAT);
		if (fd < 0) {
			perror(argv[2]);
			return 1;
		}

		write(fd, "Hello World\n", 12);

		/* sync data to disk */
		syncfs(fd);

		/* cause a kernel panic */
		// system("echo c > /proc/sysrq-trigger");
	} else {
		char buf[13];
		fd = open(argv[2], O_RDONLY);
		if (fd < 0) {
			perror(argv[2]);
			return 1;
		}

		memset(buf, 0 , sizeof(buf));
		read(fd, buf, 12);
		if (strcmp(buf, "Hello World\n")) {
			fprintf(stderr, "Output does not match: %s\n", buf);
		} else {
			fprintf(stderr, "%s has no corruption\n", argv[2]);
		}
	}

	if (close(fd) < 0)
		return 1;

	return 0;
}
