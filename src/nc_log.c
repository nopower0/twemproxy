/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <nc_core.h>
#include <nc_util.h>

static struct logger logger;

int
log_init(int level, char *name, int log_limit)
{
    struct logger *l = &logger;

    l->level = MAX(LOG_EMERG, MIN(level, LOG_PVERB));
    l->name = name;
    l->limit = log_limit;
    memset(l->count, 0, sizeof(l->count));
    if (name == NULL || !strlen(name)) {
        l->fd = STDERR_FILENO;
    } else {
        l->fd = open(name, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (l->fd < 0) {
            log_stderr("opening log file '%s' failed: %s", name,
                       strerror(errno));
            return -1;
        }
    }

    return 0;
}

void
log_deinit(void)
{
    struct logger *l = &logger;

    if (l->fd < 0 || l->fd == STDERR_FILENO) {
        return;
    }

    close(l->fd);
}

void
log_reopen(void)
{
    struct logger *l = &logger;

    if (l->fd != STDERR_FILENO) {
        close(l->fd);
        l->fd = open(l->name, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (l->fd < 0) {
            log_stderr("reopening log file '%s' failed, ignored: %s", l->name,
                       strerror(errno));
        }
    }
}

void
log_level_up(void)
{
    struct logger *l = &logger;

    if (l->level < LOG_PVERB) {
        l->level++;
        loga("up log level to %d", l->level);
    }
}

void
log_level_down(void)
{
    struct logger *l = &logger;

    if (l->level > LOG_EMERG) {
        l->level--;
        loga("down log level to %d", l->level);
    }
}

void
log_level_set(int level)
{
    struct logger *l = &logger;

    l->level = MAX(LOG_EMERG, MIN(level, LOG_PVERB));
    loga("set log level to %d", l->level);
}

int
log_loggable(int level)
{
    struct logger *l = &logger;

    if (level > l->level) {
        return 0;
    }

    return 1;
}

const char *
_log_level_str(int level)
{
    static const char * _level_str[] = {
        " 0 EMERG",
        " 1 ALERT",
        " 2 CRIT",
        " 3 ERROR",
        " 4 WARN",
        " 5 NOTICE",
        " 6 INFO",
        " 7 DEBUG",
        " 8 VERB",
        " 9 VVERB",
        " 10 VVVERB",
        " 11 PVERB"
    };
    if (level < 0 || level > LOG_PVERB) {
        return "";
    } else {
        return _level_str[level];
    }
}

#define POSITIVE(n) ((n) > 0 ? (n) : 0)
bool
_log_reach_limit(int level, int64_t usec)
{
    struct logger *l = &logger;
    int suppressed;
    int i;

    /* control limit ever 100ms */
    if (usec / 100000 > l->last_time / 100000) {
        suppressed = 0;
        i = 0;
        for (i = 0; i < LOG_N_LEVEL; ++i) {
            suppressed += POSITIVE(l->count[i] - l->limit);
        }
        if (suppressed > 0) {
            loga("[LOG SUPPRESSED: 0:%d, 1:%d, 2:%d, 3:%d, 4:%d, 5:%d, "
                "6:%d, 7:%d, 8:%d, 9:%d, 10:%d, 11:%d]",
                POSITIVE(l->count[0] - l->limit), POSITIVE(l->count[1] - l->limit),
                POSITIVE(l->count[2] - l->limit), POSITIVE(l->count[3] - l->limit),
                POSITIVE(l->count[4] - l->limit), POSITIVE(l->count[5] - l->limit),
                POSITIVE(l->count[6] - l->limit), POSITIVE(l->count[7] - l->limit),
                POSITIVE(l->count[8] - l->limit), POSITIVE(l->count[9] - l->limit),
                POSITIVE(l->count[10] - l->limit), POSITIVE(l->count[11] - l->limit));
        }
        memset(&l->count, 0, sizeof(l->count));
        l->last_time = usec;
    }

    l->count[level]++;
    if (l->count[level] == l->limit + 1) {
        loga("[LOG LEVEL %d REACHING LIMIT %d]", level, l->limit);
        return true;
    } else if (l->count[level] > l->limit + 1) {
        return true; /* suppress these logs */
    } else {
        return false;
    }
}

void
_log(int level, const char *file, int line, int panic, const char *fmt, ...)
{
    struct logger *l = &logger;
    int len, size, errno_save;
    char buf[LOG_MAX_LEN];
    char timestr[64];
    va_list args;
    int64_t usec;
    time_t t;
    struct tm *local;
    ssize_t n;

    if (l->fd < 0) {
        return;
    }

    errno_save = errno;
    len = 0;            /* length of output buffer */
    size = LOG_MAX_LEN; /* size of output buffer */

    usec = nc_usec_now();
    t = usec / 1000000;
    local = localtime(&t);
    strftime(timestr, sizeof(timestr), "%Y-%m-%d %H:%M:%S", local);

    if (level > 0 && level < LOG_N_LEVEL
            && l->limit > 0 && _log_reach_limit(level, usec)) {
        return;
    }

    len += nc_scnprintf(buf + len, size - len, "%.*s.%06d%s %s:%d ",
                        strlen(timestr), timestr, usec % 1000000,
                        _log_level_str(level), file, line);

    va_start(args, fmt);
    len += nc_vscnprintf(buf + len, size - len, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    n = nc_write(l->fd, buf, len);
    if (n < 0) {
        l->nerror++;
    }

    errno = errno_save;

    if (panic) {
        abort();
    }
}

void
_log_stderr(const char *fmt, ...)
{
    struct logger *l = &logger;
    int len, size, errno_save;
    char buf[4 * LOG_MAX_LEN];
    va_list args;
    ssize_t n;

    errno_save = errno;
    len = 0;                /* length of output buffer */
    size = 4 * LOG_MAX_LEN; /* size of output buffer */

    va_start(args, fmt);
    len += nc_vscnprintf(buf, size, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    n = nc_write(STDERR_FILENO, buf, len);
    if (n < 0) {
        l->nerror++;
    }

    errno = errno_save;
}

/*
 * Hexadecimal dump in the canonical hex + ascii display
 * See -C option in man hexdump
 */
void
_log_hexdump(const char *file, int line, char *data, int datalen,
             const char *fmt, ...)
{
    struct logger *l = &logger;
    char buf[8 * LOG_MAX_LEN];
    int i, off, len, size, errno_save;
    ssize_t n;

    if (l->fd < 0) {
        return;
    }

    /* log hexdump */
    errno_save = errno;
    off = 0;                  /* data offset */
    len = 0;                  /* length of output buffer */
    size = 8 * LOG_MAX_LEN;   /* size of output buffer */

    while (datalen != 0 && (len < size - 1)) {
        char *save, *str;
        unsigned char c;
        int savelen;

        len += nc_scnprintf(buf + len, size - len, "%08x  ", off);

        save = data;
        savelen = datalen;

        for (i = 0; datalen != 0 && i < 16; data++, datalen--, i++) {
            c = (unsigned char)(*data);
            str = (i == 7) ? "  " : " ";
            len += nc_scnprintf(buf + len, size - len, "%02x%s", c, str);
        }
        for ( ; i < 16; i++) {
            str = (i == 7) ? "  " : " ";
            len += nc_scnprintf(buf + len, size - len, "  %s", str);
        }

        data = save;
        datalen = savelen;

        len += nc_scnprintf(buf + len, size - len, "  |");

        for (i = 0; datalen != 0 && i < 16; data++, datalen--, i++) {
            c = (unsigned char)(isprint(*data) ? *data : '.');
            len += nc_scnprintf(buf + len, size - len, "%c", c);
        }
        len += nc_scnprintf(buf + len, size - len, "|\n");

        off += 16;
    }

    n = nc_write(l->fd, buf, len);
    if (n < 0) {
        l->nerror++;
    }

    errno = errno_save;
}
