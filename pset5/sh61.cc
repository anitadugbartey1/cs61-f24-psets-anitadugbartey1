#include "sh61.hh"
#include <cstring>
#include <cerrno>
#include <vector>
#include <memory>
#include <map>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>

#undef exit
#define exit __DO_NOT_CALL_EXIT__READ_PROBLEM_SET_DESCRIPTION__

// Forward declarations
class PipeManager;
class ProcessManager;
class CommandExecutor;

// Represents a file descriptor that needs to be redirected
struct Redirection {
    int from_fd;
    int to_fd;
    std::string filename;
    bool is_pipe;
};

// Manages process state and execution
class ProcessState {
    std::vector<std::string> args_;
    std::vector<Redirection> redirections_;
    pid_t pid_ = -1;
    bool is_background_ = false;
    int exit_status_ = 0;

public:
    void add_arg(const std::string& arg) { args_.push_back(arg); }
    void add_redirection(Redirection r) { redirections_.push_back(r); }
    const std::vector<std::string>& args() const { return args_; }
    pid_t pid() const { return pid_; }
    void set_pid(pid_t p) { pid_ = p; }
    bool is_background() const { return is_background_; }
    void set_background(bool bg) { is_background_ = bg; }
    int exit_status() const { return exit_status_; }
    void set_exit_status(int status) { exit_status_ = status; }
    const std::vector<Redirection>& redirections() const { return redirections_; }

    bool is_builtin() const {
        return !args_.empty() && args_[0] == "cd";
    }

    bool has_file_redirection(int fd) const {
        for (const auto& redir : redirections_) {
            if (!redir.is_pipe && redir.from_fd == fd) {
                return true;
            }
        }
        return false;
    }

    void cleanup() {
        for (const auto& redir : redirections_) {
            if (!redir.is_pipe && redir.to_fd > 2) {
                close(redir.to_fd);
            }
        }
    }
};

// Manages pipeline operations
class PipeManager {
    std::vector<int> pipe_fds_;

public:
    ~PipeManager() {
        for (int fd : pipe_fds_) {
            close(fd);
        }
    }

    void create_pipe(ProcessState& producer, ProcessState& consumer) {
        int pipefd[2];
        if (pipe(pipefd) < 0) {
            perror("pipe");
            return;
        }

        // Set close-on-exec flag
        fcntl(pipefd[0], F_SETFD, FD_CLOEXEC);
        fcntl(pipefd[1], F_SETFD, FD_CLOEXEC);

        producer.add_redirection({STDOUT_FILENO, pipefd[1], "", true});
        consumer.add_redirection({STDIN_FILENO, pipefd[0], "", true});
        
        pipe_fds_.push_back(pipefd[0]);
        pipe_fds_.push_back(pipefd[1]);
    }

    void close_all() {
        for (int fd : pipe_fds_) {
            close(fd);
        }
        pipe_fds_.clear();
    }
};

// Manages process execution and status tracking
class ProcessManager {
public:
    int execute_process(ProcessState& proc) {
        if (proc.args().empty()) return 0;

        if (proc.is_builtin()) {
            return handle_builtin(proc);
        }

        proc.set_pid(fork());
        if (proc.pid() < 0) {
            perror("fork");
            return 1;
        }

        if (proc.pid() == 0) {  // Child process
            setup_redirections(proc);
            run_command(proc);
            _exit(1);  // Should never reach here
        }

        // Parent process
        cleanup_redirections(proc);
        return 0;
    }

private:
    int handle_builtin(ProcessState& proc) {
        if (proc.args()[0] == "cd") {
            const char* dir = proc.args().size() > 1 ? proc.args()[1].c_str() : getenv("HOME");
            if (!dir) {
                fprintf(stderr, "cd: HOME not set\n");
                return 1;
            }

            // Save original file descriptors
            int saved_stdout = dup(STDOUT_FILENO);
            int saved_stderr = dup(STDERR_FILENO);

            // Set up redirections
            setup_redirections(proc);

            int status = 0;
            if (chdir(dir) < 0) {
                status = 1;
            }

            // Restore original file descriptors
            dup2(saved_stdout, STDOUT_FILENO);
            dup2(saved_stderr, STDERR_FILENO);
            close(saved_stdout);
            close(saved_stderr);

            // Clean up any redirections
            cleanup_redirections(proc);
            
            return status;
        }
        return 1;
    }

    void setup_redirections(ProcessState& proc) {
        // First handle file redirections
        for (const auto& redir : proc.redirections()) {
            if (!redir.is_pipe && !redir.filename.empty()) {
                int flags = (redir.from_fd == STDIN_FILENO) ? O_RDONLY : (O_WRONLY | O_CREAT | O_TRUNC);
                int fd = open(redir.filename.c_str(), flags, 0666);
                if (fd < 0) {
                    fprintf(stderr, "%s: %s\n", redir.filename.c_str(), strerror(errno));
                    _exit(1);
                }
                dup2(fd, redir.from_fd);
                close(fd);
            }
        }

        // Then handle pipe redirections, but only if there's no file redirection for that fd
        for (const auto& redir : proc.redirections()) {
            if (redir.is_pipe && !proc.has_file_redirection(redir.from_fd)) {
                dup2(redir.to_fd, redir.from_fd);
                close(redir.to_fd);
            }
        }
    }

    void cleanup_redirections(ProcessState& proc) {
        for (const auto& redir : proc.redirections()) {
            if ((redir.is_pipe || !redir.filename.empty()) && redir.to_fd > 2) {
                close(redir.to_fd);
            }
        }
    }

    void run_command(ProcessState& proc) {
        std::vector<char*> c_args;
        for (const auto& arg : proc.args()) {
            c_args.push_back(const_cast<char*>(arg.c_str()));
        }
        c_args.push_back(nullptr);

        execvp(c_args[0], c_args.data());
        fprintf(stderr, "%s: command not found\n", c_args[0]);
    }
};

// Main command executor that handles all command types
class CommandExecutor {
    ProcessManager proc_mgr_;
    std::vector<std::unique_ptr<ProcessState>> processes_;

public:
    int run_pipeline(shell_parser pipeline) {
        processes_.clear();
        
        // Parse all commands in pipeline
        for (shell_parser cmd = pipeline.first_command(); cmd; cmd.next_command()) {
            auto proc = std::make_unique<ProcessState>();
            parse_command(cmd, *proc);
            processes_.push_back(std::move(proc));
        }

        if (processes_.empty()) return 0;

        // Set up pipes between processes
        PipeManager pipe_mgr;
        for (size_t i = 0; i < processes_.size() - 1; ++i) {
            pipe_mgr.create_pipe(*processes_[i], *processes_[i + 1]);
        }

        // Execute processes in reverse order
        for (auto it = processes_.rbegin(); it != processes_.rend(); ++it) {
            int status = proc_mgr_.execute_process(**it);
            if ((*it)->is_builtin()) {
                (*it)->set_exit_status(status);
            }
        }

        // Close all pipes in parent
        pipe_mgr.close_all();

        // Wait for completion and get status
        int status = 0;
        if (!processes_.empty()) {
            auto& last_proc = processes_.back();
            if (last_proc->is_builtin()) {
                status = last_proc->exit_status();
            } else if (last_proc->pid() > 0) {
                waitpid(last_proc->pid(), &status, 0);
            }
            
            // Clean up other processes
            for (size_t i = 0; i < processes_.size() - 1; ++i) {
                if (processes_[i]->pid() > 0) {
                    waitpid(processes_[i]->pid(), nullptr, 0);
                }
            }
        }

        return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
    }

    int run_conditional(shell_parser conditional) {
        int last_status = 0;
        int prev_operator = TYPE_SEQUENCE;

        for (shell_parser pipeline = conditional.first_pipeline(); 
             pipeline; 
             pipeline.next_pipeline()) {
            
            int next_operator = pipeline.op();
            bool should_run = true;

            if (prev_operator == TYPE_AND && last_status != 0) {
                should_run = false;
            } else if (prev_operator == TYPE_OR && last_status == 0) {
                should_run = false;
            }

            if (should_run) {
                last_status = run_pipeline(pipeline);
                
                // If this is an OR operator and the command failed, 
                // we should continue with the next command
                if (next_operator == TYPE_OR && last_status != 0) {
                    should_run = true;
                }
            }

            prev_operator = next_operator;
        }

        return last_status;
    }

    void run_list(shell_parser parser) {
        for (shell_parser conditional = parser.first_conditional(); 
             conditional; 
             conditional.next_conditional()) {
            
            bool is_background = (conditional.op() == TYPE_BACKGROUND);
            
            if (is_background) {
                pid_t bg_pid = fork();
                if (bg_pid < 0) {
                    perror("fork");
                    return;
                }
                
                if (bg_pid == 0) {
                    setpgid(0, 0);
                    _exit(run_conditional(conditional));
                }
            } else {
                run_conditional(conditional);
            }
        }
    }

private:
    void parse_command(shell_parser cmd_parser, ProcessState& proc) {
        for (auto tok = cmd_parser.first_token(); tok; tok.next()) {
            if (tok.type() == TYPE_NORMAL) {
                proc.add_arg(tok.str());
            } 
            else if (tok.type() == TYPE_REDIRECT_OP) {
                std::string op = tok.str();
                tok.next();
                if (!tok || tok.type() != TYPE_NORMAL) continue;

                int from_fd;
                if (op == "<") from_fd = STDIN_FILENO;
                else if (op == ">") from_fd = STDOUT_FILENO;
                else if (op == "2>") from_fd = STDERR_FILENO;
                else continue;

                proc.add_redirection({from_fd, -1, tok.str(), false});
            }
        }
    }
};

int main(int argc, char* argv[]) {
    FILE* command_file = stdin;
    bool quiet = false;

    if (argc > 1 && strcmp(argv[1], "-q") == 0) {
        quiet = true;
        --argc, ++argv;
    }

    if (argc > 1) {
        command_file = fopen(argv[1], "rb");
        if (!command_file) {
            perror(argv[1]);
            return 1;
        }
    }

    claim_foreground(0);
    set_signal_handler(SIGTTOU, SIG_IGN);

    char buf[BUFSIZ];
    int bufpos = 0;
    bool needprompt = true;

    CommandExecutor executor;

    while (!feof(command_file)) {
        while (waitpid(-1, nullptr, WNOHANG) > 0) {}

        if (needprompt && !quiet) {
            printf("sh61[%d]$ ", getpid());
            fflush(stdout);
            needprompt = false;
        }

        if (fgets(&buf[bufpos], BUFSIZ - bufpos, command_file) == nullptr) {
            if (ferror(command_file) && errno == EINTR) {
                clearerr(command_file);
                buf[bufpos] = 0;
            } else {
                if (ferror(command_file)) {
                    perror("sh61");
                }
                break;
            }
        }

        bufpos = strlen(buf);
        if (bufpos == BUFSIZ - 1 || (bufpos > 0 && buf[bufpos - 1] == '\n')) {
            executor.run_list(shell_parser{buf});
            bufpos = 0;
            needprompt = true;
        }
    }

    while (waitpid(-1, nullptr, WNOHANG) > 0) {}

    if (command_file != stdin) {
        fclose(command_file);
    }

    return 0;
}