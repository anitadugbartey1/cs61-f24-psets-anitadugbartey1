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
   bool reverse_mode;         // Whether we're in reverse mode
   off_t file_size;           // Cached file size
   bool size_known;           // Whether file size is known
};

static void init_filesize(io61_file* f) {
    if (!f->size_known) {
        struct stat st;
        if (fstat(f->fd, &st) == 0 && S_ISREG(st.st_mode)) {
            f->file_size = st.st_size;
            f->size_known = true;
        }
    }
}

static int io61_fill_buffer(io61_file* f) {
    if (f->reverse_mode) {
        // Ensure we know the file size
        if (!f->size_known) {
            init_filesize(f);
            if (!f->size_known) {
                errno = ESPIPE;  // Illegal seek
                return -1;
            }
        }

        if (f->pos <= 0) {
            f->cbuf_size = 0;
            return 0;  // EOF
        }

        // Calculate how much to read
        size_t desired_size = BUFFER_SIZE;
        if (f->pos < (off_t)desired_size) {
            desired_size = f->pos;
        }

        off_t read_pos = f->pos - desired_size;

        // Seek to the correct position
        if (lseek(f->fd, read_pos, SEEK_SET) < 0) {
            return -1;
        }

        // Read the block
        ssize_t nr = read(f->fd, f->cbuf, desired_size);
        if (nr <= 0) {
            return nr;
        }

        f->cbuf_size = nr;
        f->cbuf_pos = f->cbuf_size;  // Start from end
        f->tag_position = read_pos + nr;
        return nr;
    } else {
        // Forward reading
        if (f->pos != f->tag_position) {
            if (lseek(f->fd, f->pos, SEEK_SET) < 0) {
                return -1;
            }
            f->tag_position = f->pos;
        }

        f->cbuf_pos = 0;
        f->cbuf_size = read(f->fd, f->cbuf, BUFFER_SIZE);
        if (f->cbuf_size > 0) {
            f->tag_position += f->cbuf_size;
        }
        return f->cbuf_size;
    }
}

io61_file* io61_fdopen(int fd, int mode) {
   assert(fd >= 0);
   io61_file* f = new io61_file;
   f->fd = fd;
   f->mode = mode;
   f->cbuf_size = 0;
   f->cbuf_pos = (mode == O_WRONLY || mode == O_RDWR) ? BUFFER_SIZE : 0;
   f->tag_position = 0;
   f->pos = 0;
   f->reverse_mode = false;
   f->size_known = false;
   f->file_size = 0;
   return f;
}

int io61_close(io61_file* f) {
   io61_flush(f);
   int r = close(f->fd);
   delete f;
   return r;
}

int io61_readc(io61_file* f) {
    unsigned char ch;
    ssize_t nr = io61_read(f, &ch, 1);
    if (nr == 1) {
        return ch;
    } else {
        return -1;
    }
}

ssize_t io61_read(io61_file* f, unsigned char* buf, size_t sz) {
   size_t nread = 0;

   while (nread < sz) {
       if (f->reverse_mode) {
           if (f->cbuf_pos == 0) {
               ssize_t nr = io61_fill_buffer(f);
               if (nr <= 0) break;
           }

           size_t avail = f->cbuf_pos;
           size_t to_copy = std::min(sz - nread, avail);
           memcpy(buf + nread, f->cbuf + f->cbuf_pos - to_copy, to_copy);
           f->cbuf_pos -= to_copy;
           f->pos -= to_copy;
           nread += to_copy;
       } else {
           if (f->cbuf_pos >= f->cbuf_size) {
               ssize_t nr = io61_fill_buffer(f);
               if (nr <= 0) break;
           }

           size_t avail = f->cbuf_size - f->cbuf_pos;
           size_t to_copy = std::min(sz - nread, avail);
           memcpy(buf + nread, f->cbuf + f->cbuf_pos, to_copy);
           f->cbuf_pos += to_copy;
           f->pos += to_copy;
           nread += to_copy;
       }
   }

   return (nread > 0 || sz == 0) ? nread : -1;
}

ssize_t io61_write(io61_file* f, const unsigned char* buf, size_t sz) {
   size_t nwritten = 0;

   if (f->reverse_mode) {
       // Reverse writing
       while (nwritten < sz) {
           if (f->cbuf_pos == 0) {
               if (io61_flush(f) == -1) {
                   if (nwritten == 0) return -1;
                   break;
               }
           }

           size_t space = f->cbuf_pos;
           size_t to_copy = std::min(sz - nwritten, space);
           f->cbuf_pos -= to_copy;
           memcpy(f->cbuf + f->cbuf_pos, buf + nwritten, to_copy);
           f->pos -= to_copy;
           nwritten += to_copy;
       }
   } else {
       // Forward writing
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
   }

   return nwritten;
}

int io61_flush(io61_file* f) {
   if (f->mode != O_RDONLY) {
       size_t data_size = 0;
       unsigned char* data_ptr = nullptr;
       off_t write_pos = 0;

       if (f->reverse_mode) {
           if (f->cbuf_pos < BUFFER_SIZE) {
               data_size = BUFFER_SIZE - f->cbuf_pos;
               data_ptr = f->cbuf + f->cbuf_pos;
               write_pos = f->pos;
           }
       } else {
           if (f->cbuf_pos > 0) {
               data_size = f->cbuf_pos;
               data_ptr = f->cbuf;
               write_pos = f->pos - f->cbuf_pos;
           }
       }

       if (data_size > 0) {
           if (lseek(f->fd, write_pos, SEEK_SET) == -1) return -1;

           size_t total_written = 0;
           while (total_written < data_size) {
               ssize_t nw = write(f->fd, data_ptr + total_written, data_size - total_written);
               if (nw <= 0) return -1;
               total_written += nw;
           }

           f->tag_position = write_pos + (f->reverse_mode ? 0 : total_written);
       }

       // Reset buffer positions
       if (f->reverse_mode) {
           f->cbuf_pos = BUFFER_SIZE;
           f->cbuf_size = BUFFER_SIZE;
       } else {
           f->cbuf_pos = 0;
           f->cbuf_size = 0;
       }
   }
   return 0;
}

int io61_seek(io61_file* f, off_t pos) {
    // Validate position
    if (pos < 0) {
        errno = EINVAL;
        return -1;
    }

    // Determine direction
    bool new_reverse_mode = (pos < f->pos);

    // Flush any pending writes
    if (io61_flush(f) < 0) {
        return -1;
    }

    // Update file state
    f->pos = pos;
    f->tag_position = pos;
    f->cbuf_size = 0;

    if (new_reverse_mode) {
        f->reverse_mode = true;
        f->cbuf_pos = BUFFER_SIZE;
        f->cbuf_size = BUFFER_SIZE;
    } else {
        f->reverse_mode = false;
        f->cbuf_pos = 0;
        f->cbuf_size = 0;
    }

    return 0;
}

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