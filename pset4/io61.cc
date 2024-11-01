#include "io61.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <climits>
#include <cerrno>

// Define buffer size constant
static constexpr size_t BUFFER_SIZE = 32768;  // 32KB buffer

struct io61_file {
   int fd = -1;     // file descriptor
   int mode;        // open mode
   
   // Main buffer for reading/writing
   unsigned char cbuf[BUFFER_SIZE];
   size_t cbuf_size;          // Valid bytes in buffer
   size_t cbuf_pos;           // Current position in buffer
   off_t tag_position;        // File offset of buffer start
   off_t pos;                 // Current logical file position
   
   // State tracking
   bool reverse_mode;         // Whether we're reading backwards
   off_t file_size;          // Cache file size for better seek decisions
   bool size_known;          // Whether file size is known
};

// Initialize file size if needed
static void init_filesize(io61_file* f) {
    if (!f->size_known) {
        struct stat st;
        if (fstat(f->fd, &st) == 0) {
            if (S_ISREG(st.st_mode)) {
                f->file_size = st.st_size;
                f->size_known = true;
            } else {
                // Try to get size using seek
                off_t end = lseek(f->fd, 0, SEEK_END);
                if (end >= 0) {
                    f->file_size = end;
                    f->size_known = true;
                    // Return to original position
                    lseek(f->fd, f->pos, SEEK_SET);
                }
            }
        }
    }
}

static int io61_fill_buffer(io61_file* f) {
    // Ensure we know the file size for reverse mode
    if (f->reverse_mode && !f->size_known) {
        init_filesize(f);
    }

    // If reading in reverse
    if (f->reverse_mode) {
        // Early return if at start of file
        if (f->pos <= 0) {
            return 0;  // EOF
        }

        // Calculate read position and size
        off_t read_size = BUFFER_SIZE;
        if (f->pos < read_size) {
            read_size = f->pos;  // Adjust if near start of file
        }
        off_t target_pos = f->pos - read_size;

        // Seek and read
        off_t seek_result = lseek(f->fd, target_pos, SEEK_SET);
        if (seek_result == -1) return -1;

        ssize_t nr = read(f->fd, f->cbuf, read_size);
        if (nr <= 0) return nr;

        f->cbuf_size = nr;
        f->cbuf_pos = nr - 1;  // Start from end
        f->tag_position = target_pos;
        return nr;
    } else {
        // Regular forward reading
        if (f->pos != f->tag_position) {
            off_t seek_result = lseek(f->fd, f->pos, SEEK_SET);
            if (seek_result == -1) return -1;
            f->tag_position = seek_result;
        }
        
        f->cbuf_pos = 0;
        f->cbuf_size = read(f->fd, f->cbuf, BUFFER_SIZE);
        if (f->cbuf_size > 0) {
            f->tag_position += f->cbuf_size;
        }
        return f->cbuf_size;
    }
}

// io61_fdopen(fd, mode)
//    Returns a new io61_file for file descriptor `fd`. `mode` is either
//    O_RDONLY for a read-only file or O_WRONLY for a write-only file.

io61_file* io61_fdopen(int fd, int mode) {
   assert(fd >= 0);
   io61_file* f = new io61_file;
   f->fd = fd;
   f->mode = mode;
   f->cbuf_size = 0;
   f->cbuf_pos = 0;
   f->tag_position = 0;
   f->pos = 0;
   f->reverse_mode = false;
   f->size_known = false;
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

// io61_readc(f)
//    Reads a single (unsigned) byte from `f` and returns it. Returns EOF (-1)
//    on end of file or error.

int io61_readc(io61_file* f) {
    if (f->cbuf_pos >= f->cbuf_size || f->cbuf_size == 0 ||
        (f->reverse_mode && f->cbuf_pos == size_t(-1))) {
        ssize_t nr = io61_fill_buffer(f);
        if (nr <= 0) {
            return -1;
        }
    }

    int ch;
    if (f->reverse_mode) {
        // Check for buffer underflow
        if (f->cbuf_pos == size_t(-1)) {
            return -1;
        }
        ch = f->cbuf[f->cbuf_pos--];
        f->pos--;
    } else {
        ch = f->cbuf[f->cbuf_pos++];
        f->pos++;
    }
    return (unsigned char) ch;
}

// io61_read(f, buf, sz)
//    Reads up to `sz` bytes from `f` into `buf`. Returns the number of
//    bytes read on success.

ssize_t io61_read(io61_file* f, unsigned char* buf, size_t sz) {
   size_t nread = 0;
   
   // For very large reads, bypass buffer
   if (sz >= BUFFER_SIZE * 4 && !f->reverse_mode) {  // Only bypass for forward reads
       // Flush any buffered data first
       if (f->cbuf_pos < f->cbuf_size) {
           size_t avail = f->cbuf_size - f->cbuf_pos;
           size_t to_copy = std::min(sz - nread, avail);
           memcpy(buf, f->cbuf + f->cbuf_pos, to_copy);
           f->cbuf_pos += to_copy;
           f->pos += to_copy;
           nread += to_copy;
       }
       
       // Direct read for remaining data
       while (nread < sz) {
           size_t to_read = std::min(sz - nread, size_t(1024 * 1024));  // 1MB chunks
           ssize_t nr = read(f->fd, buf + nread, to_read);
           if (nr <= 0) break;
           nread += nr;
           f->pos += nr;
           f->tag_position = f->pos;
       }
       
       return (nread > 0 || sz == 0) ? nread : -1;
   }
   
   // Normal buffered reading
   while (nread < sz) {
       // Use buffered data if available
       if (f->cbuf_pos < f->cbuf_size) {
           size_t avail = f->cbuf_size - f->cbuf_pos;
           size_t to_copy = std::min(sz - nread, avail);
           memcpy(buf + nread, f->cbuf + f->cbuf_pos, to_copy);
           f->cbuf_pos += to_copy;
           f->pos += to_copy;
           nread += to_copy;
           continue;
       }
       
       // Need to fill buffer
       ssize_t nr = io61_fill_buffer(f);
       if (nr <= 0) break;
   }
   
   return (nread > 0 || sz == 0 || errno == 0) ? nread : -1;
}

// io61_write(f, buf, sz)
//    Writes `sz` bytes from `buf` to `f`. Returns `sz` on success.

ssize_t io61_write(io61_file* f, const unsigned char* buf, size_t sz) {
   size_t nwritten = 0;
   
   // For very large writes, bypass buffer
   if (sz >= BUFFER_SIZE * 4) {
       // Flush any buffered data first
       if (f->cbuf_pos > 0) {
           if (io61_flush(f) == -1) return -1;
       }
       
       // Write in large chunks
       while (nwritten < sz) {
           size_t chunk_size = std::min(sz - nwritten, size_t(1024 * 1024));  // 1MB chunks
           ssize_t nw = write(f->fd, buf + nwritten, chunk_size);
           if (nw <= 0) {
               if (nwritten == 0) return -1;
               break;
           }
           nwritten += nw;
           f->pos += nw;
           f->tag_position = f->pos;
       }
       return nwritten;
   }
   
   // Buffered writing
   while (nwritten < sz) {
       if (f->cbuf_pos >= BUFFER_SIZE) {
           if (io61_flush(f) == -1) {
               if (nwritten == 0) return -1;
               break;
           }
       }
       
       size_t space = BUFFER_SIZE - f->cbuf_pos;
       size_t to_copy = std::min(sz - nwritten, space);
       memcpy(f->cbuf + f->cbuf_pos, buf + nwritten, to_copy);
       f->cbuf_pos += to_copy;
       f->pos += to_copy;
       nwritten += to_copy;
   }
   
   return nwritten;
}

// io61_flush(f)
//    Forces a write of all cached data written to `f`.

int io61_flush(io61_file* f) {
   if (f->mode != O_RDONLY && f->cbuf_pos > 0) {
       if (f->pos - f->cbuf_pos != f->tag_position - f->cbuf_size) {
           off_t seek_result = lseek(f->fd, f->pos - f->cbuf_pos, SEEK_SET);
           if (seek_result == -1) return -1;
           f->tag_position = seek_result;
       }
       
       size_t total_written = 0;
       while (total_written < f->cbuf_pos) {
           ssize_t nw = write(f->fd, f->cbuf + total_written, f->cbuf_pos - total_written);
           if (nw <= 0) return -1;
           total_written += nw;
       }
       
       f->tag_position += total_written;
       f->cbuf_pos = 0;
   }
   return 0;
}

// io61_seek(f, pos, whence)
//    Seeks `f` to position `pos`. Returns the new position on success.
//    This is the full version that supports all seek modes.

static int io61_seek_full(io61_file* f, off_t pos, int whence) {
    // Calculate target position
    off_t target_pos;
    if (whence == SEEK_CUR) {
        target_pos = f->pos + pos;
    } else if (whence == SEEK_END) {
        if (!f->size_known) {
            init_filesize(f);
        }
        if (f->size_known) {
            target_pos = f->file_size + pos;
        } else {
            off_t seek_result = lseek(f->fd, pos, SEEK_END);
            if (seek_result >= 0) {
                f->pos = seek_result;
                f->cbuf_size = 0;
                return seek_result;
            }
            return -1;
        }
    } else {
        target_pos = pos;
    }

    // Don't allow negative positions
    if (target_pos < 0) {
        errno = EINVAL;
        return -1;
    }

    // If already at target, no need to change
    if (target_pos == f->pos) {
        return target_pos;
    }

    // Update reverse mode flag
    f->reverse_mode = (target_pos < f->pos);

    // Check if target is within current buffer
    off_t buf_start = f->tag_position - f->cbuf_size;
    off_t buf_end = f->tag_position;
    if (target_pos >= buf_start && target_pos < buf_end) {
        f->cbuf_pos = target_pos - buf_start;
        f->pos = target_pos;
        return target_pos;
    }

    // Flush and invalidate buffer
    io61_flush(f);
    f->cbuf_size = 0;
    f->cbuf_pos = 0;
    f->pos = target_pos;

    return target_pos;
}

// io61_seek(f, pos)
//    The simpler version that only supports SEEK_SET.
//    This is the version expected by the test files.

int io61_seek(io61_file* f, off_t pos) {
    return io61_seek_full(f, pos, SEEK_SET);
}

// Helper functions

int io61_writec(io61_file* f, int c) {
   unsigned char buf = c;
   if (io61_write(f, &buf, 1) == 1) {
       return 0;
   } else {
       return -1;
   }
}

int io61_fileno(io61_file* f) {
   return f->fd;
}

off_t io61_filesize(io61_file* f) {
   struct stat s;
   int r = fstat(f->fd, &s);
   if (r >= 0 && S_ISREG(s.st_mode)) {
       return s.st_size;
   } else {
       return -1;
   }
}

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