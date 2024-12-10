#include "io61.hh"
#include <climits>
#include <cerrno>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <sys/types.h>
#include <sys/stat.h>
#include <thread>
#include <map>



// io61_file
//    Data structure for io61 file wrappers.

struct io61_file {
    int fd = -1;     // file descriptor
    int mode;        // O_RDONLY, O_WRONLY, or O_RDWR
    bool seekable;   // is this file seekable?

    // Single-slot cache
    static constexpr off_t cbufsz = 8192;
    unsigned char cbuf[cbufsz];
    off_t tag;       // offset of first character in `cbuf`
    off_t pos_tag;   // next offset to read or write (non-positioned mode)
    off_t end_tag;   // offset one past last valid character in `cbuf`

    // Positioned mode
    //bool dirty = false;       // make this atomic (give the dirty member an atomic type.)
    std::atomic<bool> dirty = false;  //has cache been written? 
    bool positioned = false;  // is cache in positioned mode?

    // file range lock
    std::recursive_mutex m; //mutex
    std::condition_variable_any condition_v;
    std::map<off_t, off_t> locked_range_map; //Each key-value pair in the locked_range_map corresponds to a locked range in the file: key: start offset, value: end offset


};


// io61_fdopen(fd, mode)
//    Returns a new io61_file for file descriptor `fd`. `mode` is either
//    O_RDONLY for a read-only file, O_WRONLY for a write-only file,
//    or O_RDWR for a read/write file.

io61_file* io61_fdopen(int fd, int mode) {
    assert(fd >= 0);
    assert((mode & O_APPEND) == 0);
    io61_file* f = new io61_file;
    f->fd = fd;
    f->mode = mode & O_ACCMODE;
    off_t off = lseek(fd, 0, SEEK_CUR);
    if (off != -1) {
        f->seekable = true;
        f->tag = f->pos_tag = f->end_tag = off;
    } else {
        f->seekable = false;
        f->tag = f->pos_tag = f->end_tag = 0;
    }
    f->dirty = f->positioned = false;
    return f;
}


// io61_close(f)
//    Closes the io61_file `f` and releases all its resources.

int io61_close(io61_file* f) {
    io61_flush(f);
    int r = close(f->fd);
    delete f;
    return r;
}


// NORMAL READING AND WRITING FUNCTIONS

// io61_readc(f)
//    Reads a single (unsigned) byte from `f` and returns it. Returns EOF,
//    which equals -1, on end of file or error.

static int io61_fill(io61_file* f);

int io61_readc(io61_file* f) {
    assert(!f->positioned);
    std::unique_lock guard(f->m); // Locking here, if this function is called concurrently by multiple threads, placing the lock at the beginning ensures that only one thread can read or modify the position-related data (pos_tag, end_tag) at a time, preventing race conditions
    if (f->pos_tag == f->end_tag) {
        io61_fill(f);
        if (f->pos_tag == f->end_tag) {
            return -1;
        }
    }
    unsigned char ch = f->cbuf[f->pos_tag - f->tag];
    ++f->pos_tag;
    return ch;
}


// io61_read(f, buf, sz)
//    Reads up to `sz` bytes from `f` into `buf`. Returns the number of
//    bytes read on success. Returns 0 if end-of-file is encountered before
//    any bytes are read, and -1 if an error is encountered before any
//    bytes are read.
//
//    Note that the return value might be positive, but less than `sz`,
//    if end-of-file or error is encountered before all `sz` bytes are read.
//    This is called a “short read.”

ssize_t io61_read(io61_file* f, unsigned char* buf, size_t sz) {
    assert(!f->positioned);
    // again, lock at the beginning to prevent other threads from modifying pos_tag and end_tag
    std::unique_lock guard(f->m); // Locking here
    size_t nread = 0;
    while (nread != sz) {
        if (f->pos_tag == f->end_tag) {
            int r = io61_fill(f);
            if (r == -1 && nread == 0) {
                return -1;
            } else if (f->pos_tag == f->end_tag) {
                break;
            }
        }
        size_t nleft = f->end_tag - f->pos_tag;
        size_t ncopy = std::min(sz - nread, nleft);
        memcpy(&buf[nread], &f->cbuf[f->pos_tag - f->tag], ncopy);
        nread += ncopy;
        f->pos_tag += ncopy;
    }
    return nread;
}


// io61_writec(f)
//    Write a single character `c` to `f` (converted to unsigned char).
//    Returns 0 on success and -1 on error.

int io61_writec(io61_file* f, int c) {
    assert(!f->positioned);
    if (f->pos_tag == f->tag + f->cbufsz) {
        int r = io61_flush(f);
        if (r == -1) {
            return -1;
        }
    }

    // protect the update of pos_tag, end_tag, and dirty
    // no data races!
    std::unique_lock guard(f->m); // Locking here

    f->cbuf[f->pos_tag - f->tag] = c;
    ++f->pos_tag;
    ++f->end_tag;
    f->dirty = true;
    return 0;
}


// io61_write(f, buf, sz)
//    Writes `sz` characters from `buf` to `f`. Returns `sz` on success.
//    Can write fewer than `sz` characters when there is an error, such as
//    a drive running out of space. In this case io61_write returns the
//    number of characters written, or -1 if no characters were written
//    before the error occurred.

ssize_t io61_write(io61_file* f, const unsigned char* buf, size_t sz) {
    assert(!f->positioned);
    // protect the update of cbuf, pos_tag, end_tag, and dirty
    // while one thread is modifying or writing, no other thread has access!
    std::unique_lock guard(f->m);
    size_t nwritten = 0;
    while (nwritten != sz) {
        if (f->end_tag == f->tag + f->cbufsz) {
            int r = io61_flush(f);
            if (r == -1 && nwritten == 0) {
                return -1;
            } else if (r == -1) {
                break;
            }
        }
        size_t nleft = f->tag + f->cbufsz - f->pos_tag;
        size_t ncopy = std::min(sz - nwritten, nleft);
        memcpy(&f->cbuf[f->pos_tag - f->tag], &buf[nwritten], ncopy);
        f->pos_tag += ncopy;
        f->end_tag += ncopy;
        f->dirty = true;
        nwritten += ncopy;
    }
    return nwritten;
}


// io61_flush(f)
//    If `f` was opened for writes, `io61_flush(f)` forces a write of any
//    cached data written to `f`. Returns 0 on success; returns -1 if an error
//    is encountered before all cached data was written.
//
//    If `f` was opened read-only and is seekable, `io61_flush(f)` drops any
//    data cached for reading and seeks to the logical file position.

static int io61_flush_dirty(io61_file* f);
static int io61_flush_dirty_positioned(io61_file* f);
static int io61_flush_clean(io61_file* f);

int io61_flush(io61_file* f) {
    // place before accessing/modifying shared state
    std::unique_lock guard(f->m);

    if (f->dirty && f->positioned) {
        return io61_flush_dirty_positioned(f);
    } else if (f->dirty) {
        return io61_flush_dirty(f);
    } else {
        return io61_flush_clean(f);
    }
}


// io61_seek(f, off)
//    Changes the file pointer for file `f` to `off` bytes into the file.
//    Returns 0 on success and -1 on failure.

int io61_seek(io61_file* f, off_t off) {
    std::unique_lock guard(f->m);
    int r = io61_flush(f);
    if (r == -1) {
        return -1;
    }
    // std::unique_lock guard(f->m);
    off_t roff = lseek(f->fd, off, SEEK_SET);
    if (roff == -1) {
        return -1;
    }
    f->tag = f->pos_tag = f->end_tag = off;
    f->positioned = false;
    return 0;
}


// Helper functions

// io61_fill(f)
//    Fill the cache by reading from the file. Returns 0 on success,
//    -1 on error. Used only for non-positioned files.

static int io61_fill(io61_file* f) {
    std::unique_lock guard(f->m);
    assert(f->tag == f->end_tag && f->pos_tag == f->end_tag);
    ssize_t nr;
    while (true) {
        nr = read(f->fd, f->cbuf, f->cbufsz);
        if (nr >= 0) {
            break;
        } else if (errno != EINTR && errno != EAGAIN) {
            return -1;
        }
    }
    f->end_tag += nr;
    return 0;
}


// io61_flush_*(f)
//    Helper functions for io61_flush.

static int io61_flush_dirty(io61_file* f) {
    // Called when `f`’s cache is dirty and not positioned.
    // Uses `write`; assumes that the initial file position equals `f->tag`.
    off_t flush_tag = f->tag;
    while (flush_tag != f->end_tag) {
        ssize_t nw = write(f->fd, &f->cbuf[flush_tag - f->tag],
                           f->end_tag - flush_tag);
        if (nw >= 0) {
            flush_tag += nw;
        } else if (errno != EINTR && errno != EINVAL) {
            return -1;
        }
    }
    f->dirty = false;
    f->tag = f->pos_tag = f->end_tag;
    return 0;
}

static int io61_flush_dirty_positioned(io61_file* f) {
    // Called when `f`’s cache is dirty and positioned.
    // Uses `pwrite`; does not change file position.
    off_t flush_tag = f->tag;
    while (flush_tag != f->end_tag) {
        ssize_t nw = pwrite(f->fd, &f->cbuf[flush_tag - f->tag],
                            f->end_tag - flush_tag, flush_tag);
        if (nw >= 0) {
            flush_tag += nw;
        } else if (errno != EINTR && errno != EINVAL) {
            return -1;
        }
    }
    f->dirty = false;
    return 0;
}

static int io61_flush_clean(io61_file* f) {
    // Called when `f`’s cache is clean.
    if (!f->positioned && f->seekable) {
        if (lseek(f->fd, f->pos_tag, SEEK_SET) == -1) {
            return -1;
        }
        f->tag = f->end_tag = f->pos_tag;
    }
    return 0;
}



// POSITIONED I/O FUNCTIONS

// io61_pread(f, buf, sz, off)
//    Read up to `sz` bytes from `f` into `buf`, starting at offset `off`.
//    Returns the number of characters read or -1 on error.
//
//    This function can only be called when `f` was opened in read/write
//    more (O_RDWR).

static int io61_pfill(io61_file* f, off_t off);

ssize_t io61_pread(io61_file* f, unsigned char* buf, size_t sz,
                   off_t off) {
    // protect the update of cbuf, tag, end_tag
    std::unique_lock guard(f->m);
    if (!f->positioned || off < f->tag || off >= f->end_tag) {
        if (io61_pfill(f, off) == -1) {
            return -1;
        }
    }
    size_t nleft = f->end_tag - off;
    size_t ncopy = std::min(sz, nleft);
    memcpy(buf, &f->cbuf[off - f->tag], ncopy);
    return ncopy;
}


// io61_pwrite(f, buf, sz, off)
//    Write up to `sz` bytes from `buf` into `f`, starting at offset `off`.
//    Returns the number of characters written or -1 on error.
//
//    This function can only be called when `f` was opened in read/write
//    more (O_RDWR).

ssize_t io61_pwrite(io61_file* f, const unsigned char* buf, size_t sz,
                    off_t off) {
    std::unique_lock guard(f->m);
    if (!f->positioned || off < f->tag || off >= f->end_tag) {
        if (io61_pfill(f, off) == -1) {
            return -1;
        }
    }
    size_t nleft = f->end_tag - off;
    size_t ncopy = std::min(sz, nleft);
    memcpy(&f->cbuf[off - f->tag], buf, ncopy);
    f->dirty = true;
    return ncopy;
}


// io61_pfill(f, off)
//    Fill the single-slot cache with data including offset `off`.
//    The handout code rounds `off` down to a multiple of 8192.

static int io61_pfill(io61_file* f, off_t off) {
    std::unique_lock guard(f->m);
    assert(f->mode == O_RDWR);
    if (f->dirty && io61_flush(f) == -1) {
        return -1;
    }

    off = off - (off % 8192);
    ssize_t nr = pread(f->fd, f->cbuf, f->cbufsz, off);
    if (nr == -1) {
        return -1;
    }
    f->tag = off;
    f->end_tag = off + nr;
    f->positioned = true;
    return 0;
}



// FILE LOCKING FUNCTIONS

// Function declaration (prototype)
bool may_overlap_with_other_lock(io61_file* f, off_t off, off_t len);

// io61_try_lock(f, off, len, locktype)
//    Attempts to acquire a lock on offsets `[off, off + len)` in file `f`.
//    `locktype` must be `LOCK_EX`, which requests an exclusive lock,
//    or `LOCK_SH`, which requests an shared lock. (The applications we
//    provide in pset 6 only ever use `LOCK_EX`; `LOCK_SH` support is extra
//    credit.)
//
//    Returns 0 if the lock was acquired and -1 if it was not. Does not
//    block: if the lock cannot be acquired, it returns -1 right away.

int io61_try_lock(io61_file* f, off_t off, off_t len, int locktype) {
    assert(off >= 0 && len >= 0);
    assert(locktype == LOCK_EX || locktype == LOCK_SH);
    if (len == 0) { //nothing to lock
        return 0;
    }
    // no data races
    std::unique_lock guard(f->m);
    if (may_overlap_with_other_lock(f, off, len)) {
        return -1;
    }
    // store a specific range entry in locked_range_map, 
    //l locks specific ranges of file
    f->locked_range_map[off] = off + len;  

    return 0;
}


// io61_lock(f, off, len, locktype)
//    Acquire a lock on offsets `[off, off + len)` in file `f`.
//    `locktype` must be `LOCK_EX`, which requests an exclusive lock,
//    or `LOCK_SH`, which requests an shared lock. (The applications we
//    provide in pset 6 only ever use `LOCK_EX`; `LOCK_SH` support is extra
//    credit.)
//
//    Returns 0 if the lock was acquired and -1 on error. Blocks until
//    the lock can be acquired; the -1 return value is reserved for true
//    error conditions, such as EDEADLK (a deadlock was detected).

int io61_lock(io61_file* f, off_t off, off_t len, int locktype) {
    assert(off >= 0 && len >= 0);
    assert(locktype == LOCK_EX || locktype == LOCK_SH);
    if (len == 0) {
        return 0;
    }
    // mutex ensures that only one thread can modify the lock state of the file at a time,
    // lock is held for the duration of the function, ensuring thread safety when modifying the file's state
    std::unique_lock guard(f->m);
    while (may_overlap_with_other_lock(f, off, len)) {
        // If there is overlap, the thread waits on the condition variable f->condition_v while holding the lock
        // the thread will release the mutex temporarily and block, allowing other threads to potentially release their locks and notify this thread when the lock can be acquired
        f->condition_v.wait(guard);
    }
    // lock is acquired for certain range by storing a specific range entry in locked_range_map, 
    f->locked_range_map[off] = off + len;  

    // The handout code polls using `io61_try_lock`.
    // while (io61_try_lock(f, off, len, locktype) != 0) {
    // }
    return 0;
}


// io61_unlock(f, off, len)
//    Release the lock on offsets `[off, off + len)` in file `f`.
//    Returns 0 on success and -1 on error.

int io61_unlock(io61_file* f, off_t off, off_t len) {
    // (void) f;
    assert(off >= 0 && len >= 0);
    if (len == 0) {
        return 0;
    }
    std::unique_lock guard(f->m);
    // remove lock by erasing the range starting at off, so if the range is locked,
    // this will unlock it by removing it from the locked_range_map
    int remove = f->locked_range_map.erase(off);
    if (remove == 0) { // lock was not found
        return -1;
    }
    // after the lock has been removed, the function notifies all threads that are waiting on the condition variable
    // could wake up other files waiting for lock on the file to be released
    f->condition_v.notify_all();  // spurious wakeup possible, prob no big deal
    return 0;
}



// HELPER FUNCTIONS
// You shouldn't need to change these functions.

// may_overlap_with_other_lock(f, off, len) predicate
// Return true if some other thread (not `std::this_thread::get_id()`)
// has a lock on a range of `f`, and that range might overlap with the
// range [off, off + len).
//
// This function may return true when another thread has a range lock on
// `f` that *doesn’t* overlap with [off, off + len). That is, it can
// be conservative, rather than precise. However, it *must return false*
// if all range locks on `f` are held by *this* thread.
//
// The caller must have locked all mutexes required to examine `f`’s
// range lock state.

bool may_overlap_with_other_lock(io61_file* f, off_t off, off_t len) {
    // 
    for (const auto& [beg, end] : f->locked_range_map) { 
        // check if beginning of the new range (off) is less than the end of an existing range (end)
        // or end of the new range (off + len) is greater than or equal to the beginning of an existing range (beg)
        // if first is false, no need to check further
        if (off < end && off + len >= beg) {
            return true;  // overlapping range found
        }
    }
    // no overlapping found
    return false;
}

// io61_open_check(filename, mode)
//    Opens the file corresponding to `filename` and returns its io61_file.
//    If `!filename`, returns either the standard input or the
//    standard output, depending on `mode`. Exits with an error message if
//    `filename != nullptr` and the named file cannot be opened.

io61_file* io61_open_check(const char* filename, int mode) {
    int fd;
    if (filename) {
        fd = open(filename, mode, 0666);
    } else if ((mode & O_ACCMODE) == O_RDONLY) {
        fd = STDIN_FILENO;
    } else {
        fd = STDOUT_FILENO;
    }
    if (fd < 0) {
        fprintf(stderr, "%s: %s\n", filename, strerror(errno));
        exit(1);
    }
    return io61_fdopen(fd, mode & O_ACCMODE);
}


// io61_fileno(f)
//    Returns the file descriptor associated with `f`.

int io61_fileno(io61_file* f) {
    return f->fd;
}


// io61_filesize(f)
//    Returns the size of `f` in bytes. Returns -1 if `f` does not have a
//    well-defined size (for instance, if it is a pipe).

off_t io61_filesize(io61_file* f) {
    struct stat s;
    int r = fstat(f->fd, &s);
    if (r >= 0 && S_ISREG(s.st_mode)) {
        return s.st_size;
    } else {
        return -1;
    }
}