// Floor: the least a server can do per request with epoll — recv, find the end
// of a request head, send a fixed 130-byte response. No HTTP parsing, no Python.
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
static const char RESP[] = "HTTP/1.1 200 OK\r\ncontent-type: text/plain; charset=utf-8\r\ndate: Tue, 10 Nov 2026 12:00:00 GMT\r\ncontent-length: 13\r\n\r\nHello, World!";
int main(int argc, char **argv) {
    int port = argc > 1 ? atoi(argv[1]) : 8200, one = 1;
    int ls = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(port), .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    if (bind(ls, (void *)&a, sizeof a) || listen(ls, 2048)) { perror("bind"); return 1; }
    int ep = epoll_create1(0);
    struct epoll_event ev = {.events = EPOLLIN, .data.fd = ls}, evs[1024];
    epoll_ctl(ep, EPOLL_CTL_ADD, ls, &ev);
    static char buf[65536], out[65536 * 2];
    for (;;) {
        int n = epoll_wait(ep, evs, 1024, -1);
        for (int i = 0; i < n; i++) {
            int fd = evs[i].data.fd;
            if (fd == ls) {
                int c;
                while ((c = accept4(ls, NULL, NULL, SOCK_NONBLOCK)) >= 0) {
                    setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
                    ev.events = EPOLLIN; ev.data.fd = c; epoll_ctl(ep, EPOLL_CTL_ADD, c, &ev);
                }
                continue;
            }
            ssize_t r = recv(fd, buf, sizeof buf, 0);
            if (r <= 0) { if (r == 0 || errno != EAGAIN) close(fd); continue; }
            // count request heads (assumes whole requests per read, GET only)
            size_t o = 0; char *p = buf, *end = buf + r;
            while ((p = memmem(p, end - p, "\r\n\r\n", 4))) { memcpy(out + o, RESP, sizeof RESP - 1); o += sizeof RESP - 1; p += 4; }
            if (o) send(fd, out, o, MSG_NOSIGNAL);
        }
    }
}
