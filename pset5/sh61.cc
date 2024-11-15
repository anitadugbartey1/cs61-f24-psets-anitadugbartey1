#include "sh61.hh"
#include <cstring>
#include <cerrno>
#include <vector>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>

#undef exit
#define exit __DO_NOT_CALL_EXIT__READ_PROBLEM_SET_DESCRIPTION__

struct command {
    std::vector<std::string> args;
    pid_t pid = -1;      
    int input_fd = STDIN_FILENO;
    int output_fd = STDOUT_FILENO;
    int error_fd = STDERR_FILENO;

    command() = default;
    ~command() {
        // Only close file descriptors if they were redirected
        if (input_fd != STDIN_FILENO) close(input_fd);
        if (output_fd != STDOUT_FILENO) close(output_fd);
        if (error_fd != STDERR_FILENO) close(error_fd);
    }

    bool is_builtin() const {
        return !args.empty() && args[0] == "cd";
    }

    int execute_builtin() {
        if (args[0] == "cd") {
            const char* dir = args.size() > 1 ? args[1].c_str() : getenv("HOME");
            if (!dir) {
                if (error_fd == STDERR_FILENO) {
                    fprintf(stderr, "cd: HOME not set\n");
                }
                return 1;
            }
            if (chdir(dir) < 0) {
                if (error_fd == STDERR_FILENO) {
                    fprintf(stderr, "cd: %s: %s\n", dir, strerror(errno));
                }
                return 1;
            }
            return 0;
        }
        return 1;
    }

    int execute() {
        if (args.empty()) return 1;

        if (is_builtin()) {
            return execute_builtin();
        }

        // Handle input redirection errors before forking
        if (input_fd != STDIN_FILENO && input_fd < 0) {
            fprintf(stderr, "No such file or directory\n");
            return 1;
        }

        pid = fork();
        if (pid < 0) {
            perror("fork");
            return 1;
        }

        if (pid == 0) {  // Child process
            // Handle redirections
            if (input_fd != STDIN_FILENO) {
                if (dup2(input_fd, STDIN_FILENO) < 0) {
                    perror("dup2");
                    _exit(1);
                }
                close(input_fd);
            }
            if (output_fd != STDOUT_FILENO) {
                if (dup2(output_fd, STDOUT_FILENO) < 0) {
                    perror("dup2");
                    _exit(1);
                }
                close(output_fd);
            }
            if (error_fd != STDERR_FILENO) {
                if (dup2(error_fd, STDERR_FILENO) < 0) {
                    perror("dup2");
                    _exit(1);
                }
                close(error_fd);
            }

            std::vector<char*> c_args;
            for (const auto& arg : args) {
                c_args.push_back(const_cast<char*>(arg.c_str()));
            }
            c_args.push_back(nullptr);

            execvp(c_args[0], c_args.data());
            fprintf(stderr, "%s: command not found\n", c_args[0]);
            _exit(1);
        }

        return 0;
    }
};

int run_pipeline(shell_parser pipeline) {
    std::vector<command*> commands;
    
    // Parse all commands in pipeline
    shell_parser cmd_parser = pipeline.first_command();
    while (cmd_parser) {
        command* cmd = new command();
        
        auto token = cmd_parser.first_token();
        while (token) {
            if (token.type() == TYPE_NORMAL) {
                cmd->args.push_back(token.str());
            } 
            else if (token.type() == TYPE_REDIRECT_OP) {
                std::string op = token.str();
                token.next();
                if (!token || token.type() != TYPE_NORMAL) {
                    fprintf(stderr, "Syntax error: missing filename after redirection\n");
                    delete cmd;
                    for (auto c : commands) delete c;
                    return 1;
                }

                std::string filename = token.str();
                int fd = -1;

                if (op == "<") {
                    fd = open(filename.c_str(), O_RDONLY);
                    if (fd >= 0 || cmd->input_fd == STDIN_FILENO) {
                        if (cmd->input_fd != STDIN_FILENO) close(cmd->input_fd);
                        cmd->input_fd = fd;
                    }
                } 
                else if (op == ">") {
                    fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
                    if (fd >= 0 || cmd->output_fd == STDOUT_FILENO) {
                        if (cmd->output_fd != STDOUT_FILENO) close(cmd->output_fd);
                        cmd->output_fd = fd;
                    }
                } 
                else if (op == "2>") {
                    fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
                    if (fd >= 0 || cmd->error_fd == STDERR_FILENO) {
                        if (cmd->error_fd != STDERR_FILENO) close(cmd->error_fd);
                        cmd->error_fd = fd;
                    }
                }

                if (fd < 0) {
                    fprintf(stderr, "%s: %s\n", filename.c_str(), strerror(errno));
                    delete cmd;
                    for (auto c : commands) delete c;
                    return 1;
                }
            }
            token.next();
        }

        if (!cmd->args.empty()) {
            commands.push_back(cmd);
        } else {
            delete cmd;
        }
        cmd_parser.next_command();
    }

    if (commands.empty()) {
        return 0;
    }

    // Handle single builtin command without pipeline
    if (commands.size() == 1 && commands[0]->is_builtin()) {
        int status = commands[0]->execute_builtin();
        delete commands[0];
        return status;
    }

    // Create pipes between commands
    std::vector<int> pipe_fds;
    for (size_t i = 0; i < commands.size() - 1; ++i) {
        int pipefd[2];
        if (pipe(pipefd) < 0) {
            perror("pipe");
            for (auto cmd : commands) delete cmd;
            return 1;
        }

        // Set close-on-exec flag for both pipe ends
        fcntl(pipefd[0], F_SETFD, FD_CLOEXEC);
        fcntl(pipefd[1], F_SETFD, FD_CLOEXEC);
        
        // Only set up pipe if no explicit redirection exists
        if (commands[i]->output_fd == STDOUT_FILENO) {
            commands[i]->output_fd = pipefd[1];
        } else {
            close(pipefd[1]);
        }
        
        if (commands[i + 1]->input_fd == STDIN_FILENO) {
            commands[i + 1]->input_fd = pipefd[0];
        } else {
            close(pipefd[0]);
        }

        pipe_fds.push_back(pipefd[0]);
        pipe_fds.push_back(pipefd[1]);
    }

    // Execute all commands in reverse order
    for (int i = commands.size() - 1; i >= 0; --i) {
        commands[i]->execute();
    }

    // Close all pipe file descriptors in the parent
    for (int fd : pipe_fds) {
        close(fd);
    }

    // Wait for completion and get status
    int status = 0;
    if (!commands.empty()) {
        waitpid(commands.back()->pid, &status, 0);
        
        // Clean up other processes
        for (size_t i = 0; i < commands.size() - 1; ++i) {
            waitpid(commands[i]->pid, nullptr, 0);
        }
    }

    // Cleanup commands
    for (auto cmd : commands) {
        delete cmd;
    }

    return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}

int run_conditional(shell_parser conditional) {
    int last_status = 0;
    shell_parser pipeline = conditional.first_pipeline();
    int prev_operator = TYPE_SEQUENCE;
    
    while (pipeline) {
        int next_operator = pipeline.op();
        bool should_run = true;

        if (prev_operator == TYPE_AND && last_status != 0) {
            should_run = false;
        } 
        else if (prev_operator == TYPE_OR && last_status == 0) {
            should_run = false;
        }

        if (should_run) {
            last_status = run_pipeline(pipeline);
        }

        prev_operator = next_operator;
        pipeline.next_pipeline();
    }

    return last_status;
}

void run_list(shell_parser parser) {
    shell_parser conditional = parser.first_conditional();
    
    while (conditional) {
        bool is_background = (conditional.op() == TYPE_BACKGROUND);
        
        if (is_background) {
            pid_t bg_pid = fork();
            if (bg_pid < 0) {
                perror("fork");
                return;
            }
            
            if (bg_pid == 0) {
                setpgid(0, 0);  // Put in its own process group
                _exit(run_conditional(conditional));
            }
        } 
        else {
            run_conditional(conditional);
        }
        
        conditional.next_conditional();
    }
}

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
            run_list(shell_parser{buf});
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