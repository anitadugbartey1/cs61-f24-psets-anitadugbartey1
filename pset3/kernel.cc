#include "kernel.hh"
#include "k-apic.hh"
#include "k-vmiter.hh"
#include "obj/k-firstprocess.h"
#include <atomic>
#include <cassert> // Necessary for the assert macro

// Implement __assert_fail for kernel
extern "C" void __assert_fail(const char* assertion, const char* file, unsigned int line, const char* function) {
    // Log the assertion failure details
    log_printf("Assertion failed: %s, file %s, line %u, function %s\n", assertion, file, line, function);
    
    // Optionally, display the panic message on the console
    console_printf(CPOS(0, 0), COLOR_ERROR, "PANIC: Assertion failed: %s, file %s, line %u, function %s\n", assertion, file, line, function);
    
    // Halt the system or enter an infinite loop to stop execution
    while (true) {
        // Optionally, implement a more graceful shutdown or recovery mechanism
    }
}

// INITIAL PHYSICAL MEMORY LAYOUT
//
//  +-------------- Base Memory --------------+
//  v                                         v
// +-----+--------------------+----------------+--------------------+---------/
// |     | Kernel      Kernel |       :    I/O | App 1        App 1 | App 2
// |     | Code + Data  Stack |  ...  : Memory | Code + Data  Stack | Code ...
// +-----+--------------------+----------------+--------------------+---------/
// 0  0x40000              0x80000 0xA0000 0x100000             0x140000
//                                             ^
//                                             | \___ PROC_SIZE ___/
//                                      PROC_START_ADDR

#define PROC_SIZE 0x40000       // initial state only

proc ptable[PID_MAX];           // array of process descriptors
                                // Note that `ptable[0]` is never used.
proc* current = nullptr;        // pointer to currently executing proc

#define HZ 100                  // timer interrupt frequency (interrupts/sec)
static std::atomic<unsigned long> ticks{0}; // # timer interrupts so far

// Memory state - see `kernel.hh`
physpageinfo physpages[NPAGES];

// Free list for memory allocation
uintptr_t free_list_head = 0;

// Function Prototypes
[[noreturn]] void schedule();
[[noreturn]] void run(proc* p);
void exception(regstate* regs);
uintptr_t syscall(regstate* regs);
void memshow();

// Kernel Initialization Helpers
static void initialize_hardware();
static void initialize_memory();
static void initialize_process_table();
static void load_initial_processes(const char* command);
static void process_setup(pid_t pid, const char* program_name);

// Memory Allocation Functions
void* kalloc(size_t sz);
void kfree(void* kptr);

// Process Management Functions
pid_t syscall_fork();
void sys_exit();

// Helper Functions
void cleanup_pagetable(x86_64_pagetable* pagetable);
int syscall_page_alloc(uintptr_t addr);

// kernel_start(command)
//    Initialize the hardware and processes and start running. The `command`
//    string is an optional string passed from the boot loader.

void kernel_start(const char* command) {
    initialize_hardware();
    log_printf("Starting WeensyOS - Alternative Kernel\n");

    ticks = 1;
    init_timer(HZ);

    // Clear screen
    console_clear();

    // Initialize memory management structures
    initialize_memory();

    // Initialize process table
    initialize_process_table();

    // Load initial processes
    load_initial_processes(command);

    // Switch to first process using run()
    run(&ptable[1]);
}

// initialize_hardware()
//    Initialize all necessary hardware components.

static void initialize_hardware() {
    init_hardware();
    // Additional hardware initialization can be added here
}

// initialize_memory()
//    Set up the initial memory mappings and free list.

static void initialize_memory() {
    // Initialize kernel page table with identity mapping
    for (uintptr_t addr = 0; addr < MEMSIZE_PHYSICAL; addr += PAGESIZE) {
        int perm = PTE_P | PTE_W;
        if (addr >= PROC_START_ADDR || addr == CONSOLE_ADDR) {
            perm |= PTE_U;
        } else if (addr == 0) {
            perm = 0; // Inaccessible
        }

        // Install identity mapping
        int r = vmiter(kernel_pagetable, addr).try_map(addr, perm);
        assert(r == 0); // mappings during kernel_start MUST NOT fail

        // Initialize free list for user pages
        if (addr >= PROC_START_ADDR && addr != CONSOLE_ADDR) {
            int page_index = addr / PAGESIZE;
            physpages[page_index].refcount = 0; // Mark as free

            // Add to free list
            *(uintptr_t*)addr = free_list_head;
            free_list_head = addr;
        }
    }
}

// initialize_process_table()
//    Sets all process table entries to free.

static void initialize_process_table() {
    for (pid_t i = 0; i < PID_MAX; ++i) {
        ptable[i].pid = i;
        ptable[i].state = P_FREE;
        ptable[i].pagetable = nullptr;
    }
}

// load_initial_processes()
//    Loads the initial user processes based on the provided command.

static void load_initial_processes(const char* command) {
    if (!command) {
        command = WEENSYOS_FIRST_PROCESS;
    }

    if (!program_image(command).empty()) {
        process_setup(1, command);
    } else {
        // Load multiple allocator processes as fallback
        process_setup(1, "allocator");
        process_setup(2, "allocator2");
        process_setup(3, "allocator3");
        process_setup(4, "allocator4");
    }
}

// process_setup(pid, program_name)
//    Load application program `program_name` as process number `pid`.

void process_setup(pid_t pid, const char* program_name) {
    init_process(&ptable[pid], 0);

    // Allocate a new empty page table for the process
    ptable[pid].pagetable = kalloc_pagetable();
    assert(ptable[pid].pagetable != nullptr);

    // Map kernel memory into the process's page table
    for (vmiter src(kernel_pagetable, 0), dst(ptable[pid].pagetable, 0);
         src.va() < PROC_START_ADDR;
         src += PAGESIZE, dst += PAGESIZE) {
        if (src.present()) {
            int r = dst.try_map(src.pa(), src.perm());
            assert(r == 0);
        }
    }

    // Obtain reference to program image
    program_image pgm(program_name);

    // Allocate and map process memory as specified in program image
    for (auto seg = pgm.begin(); seg != pgm.end(); ++seg) {
        for (uintptr_t a = round_down(seg.va(), PAGESIZE);
             a < seg.va() + seg.size();
             a += PAGESIZE) {

            if (seg.writable()) {
                // Writable segment: allocate new physical page
                void* new_page = kalloc(PAGESIZE);
                assert(new_page != nullptr);
                memset(new_page, 0, PAGESIZE); // Initialize with 0

                // Map writable page
                int perm = PTE_P | PTE_W | PTE_U;
                int r = vmiter(ptable[pid].pagetable, a).try_map((uintptr_t)new_page, perm);
                assert(r == 0);
            } else {
                // Non-writable (read-only): share page across processes
                void* shared_page = kalloc(PAGESIZE);
                assert(shared_page != nullptr);
                memset(shared_page, 0, PAGESIZE); // Initialize with 0

                // Map read-only page
                int perm = PTE_P | PTE_U;
                int r = vmiter(ptable[pid].pagetable, a).try_map((uintptr_t)shared_page, perm);
                assert(r == 0);

                // Increment reference count in `physpages`
                ++physpages[(uintptr_t)shared_page / PAGESIZE].refcount;
            }
        }
    }

    // Copy instructions and data into process memory as needed
    for (auto seg = pgm.begin(); seg != pgm.end(); ++seg) {
        uintptr_t start_ptr = (uintptr_t)seg.data();
        for (uintptr_t a = round_down(seg.va(), PAGESIZE);
             a < seg.va() + seg.size();
             a += PAGESIZE) {

            uintptr_t page = vmiter(ptable[pid].pagetable, a).pa();
            size_t copy_size = (a + PAGESIZE <= seg.va() + seg.data_size()) ? PAGESIZE : seg.data_size() % PAGESIZE;
            memcpy((void*)page, (void*)start_ptr, copy_size);
            start_ptr += copy_size;
        }
    }

    // Set entry point
    ptable[pid].regs.reg_rip = pgm.entry();

    // Set up the stack segment
    uintptr_t stack_addr = MEMSIZE_VIRTUAL - PAGESIZE;
    void* new_stack_pa = kalloc(PAGESIZE);
    memset(new_stack_pa, 0, PAGESIZE); // Initialize with 0
    assert(new_stack_pa != nullptr);

    int r = vmiter(ptable[pid].pagetable, stack_addr).try_map((uintptr_t)new_stack_pa, PTE_P | PTE_W | PTE_U);
    assert(r == 0);

    ptable[pid].regs.reg_rsp = stack_addr + PAGESIZE;
    ptable[pid].state = P_RUNNABLE;
}

// kalloc(sz)
//    Kernel physical memory allocator using a free list.

void* kalloc(size_t sz) {
    if (sz > PAGESIZE) {
        return nullptr;
    }

    // Allocate from the free list
    if (free_list_head == 0) {
        return nullptr; // No free pages available
    }

    // Remove the first page from the free list
    uintptr_t pa = free_list_head;
    free_list_head = *(uintptr_t*)pa;

    // Initialize memory to zero
    memset((void*)pa, 0, PAGESIZE);

    // Update reference count
    physpages[pa / PAGESIZE].refcount = 1;

    return (void*)pa;
}

// kfree(kptr)
//    Free `kptr`, which must have been previously returned by `kalloc`.
//    If `kptr == nullptr` does nothing.

void kfree(void* kptr) {
    if (!kptr) {
        return;
    }

    uintptr_t pa = (uintptr_t)kptr;
    assert(pa % PAGESIZE == 0); // Ensure page alignment

    int page_index = pa / PAGESIZE;
    assert(static_cast<unsigned int>(page_index) < NPAGES); // Fix signedness

    assert(physpages[page_index].refcount > 0); // Ensure the page is in use

    // Decrement reference count
    --physpages[page_index].refcount;

    // Optionally, log the freeing action
    log_printf("kfree: freeing page at 0x%lx, new refcount=%d\n", pa, physpages[page_index].refcount);

    if (physpages[page_index].refcount == 0) {
        // Add the page back to the free list
        *(uintptr_t*)pa = free_list_head;
        free_list_head = pa;

        // Clear memory for safety
        memset(kptr, 0, PAGESIZE);
    }
}

// syscall_fork()
//    Handles the fork system call to create a new process.

pid_t syscall_fork() {
    // Find a free slot for the child process
    pid_t child_pid = -1;
    for (pid_t i = 1; i < PID_MAX; ++i) { // Skip PID 0
        if (ptable[i].state == P_FREE) {
            child_pid = i;
            break;
        }
    }

    if (child_pid == -1) {
        return -1; // No free slots available
    }

    // Allocate a new page table for the child
    x86_64_pagetable* child_pagetable = kalloc_pagetable();
    if (!child_pagetable) {
        return -1; // Allocation failed
    }

    // Copy kernel mappings
    for (vmiter src(kernel_pagetable, 0), dst(child_pagetable, 0);
         src.va() < PROC_START_ADDR;
         src += PAGESIZE, dst += PAGESIZE) {
        if (src.present()) {
            int r = dst.try_map(src.pa(), src.perm());
            if (r != 0) {
                cleanup_pagetable(child_pagetable);
                return -1;
            }
        }
    }

    // Copy user space mappings
    for (vmiter src(current->pagetable, PROC_START_ADDR), dst(child_pagetable, PROC_START_ADDR);
         src.va() < MEMSIZE_VIRTUAL;
         src += PAGESIZE, dst += PAGESIZE) {
        if (!src.present()) continue;

        if (!src.writable() && src.user() && src.va() != CONSOLE_ADDR) {
            // Share read-only page
            int r = dst.try_map(src.pa(), src.perm());
            if (r != 0) {
                cleanup_pagetable(child_pagetable);
                return -1;
            }
            ++physpages[src.pa() / PAGESIZE].refcount;
        }
        else if (src.writable() && src.user() && src.va() != CONSOLE_ADDR) {
            // Copy-on-write: allocate new page and copy content
            void* new_page = kalloc(PAGESIZE);
            if (!new_page) {
                cleanup_pagetable(child_pagetable);
                return -1;
            }

            memcpy(new_page, (void*)src.pa(), PAGESIZE);
            int r = dst.try_map((uintptr_t)new_page, src.perm());
            if (r != 0) {
                kfree(new_page);
                cleanup_pagetable(child_pagetable);
                return -1;
            }
        }
        // Kernel mappings are already copied
    }

    // Set up child process descriptor
    ptable[child_pid].regs = current->regs;
    ptable[child_pid].regs.reg_rax = 0; // Child returns 0 from fork
    ptable[child_pid].pagetable = child_pagetable;
    ptable[child_pid].state = P_RUNNABLE;

    // Parent receives child's PID
    return child_pid;
}

// exception(regs)
//    Exception handler (for interrupts, traps, and faults).

void exception(regstate* regs) {
    // Save current register state
    current->regs = *regs;
    regs = &current->regs;

    // Log exception details
    log_printf("Process %d: Exception %d at RIP %p\n",
               current->pid, regs->reg_intno, regs->reg_rip);

    // Show the current cursor location and memory state
    console_show_cursor(cursorpos);
    if (regs->reg_intno != INT_PF || (regs->reg_errcode & PTE_U)) {
        memshow();
    }

    // If Control-C was typed, exit the virtual machine.
    check_keyboard();

    // Handle the exception
    switch (regs->reg_intno) {

    case INT_IRQ + IRQ_TIMER:
        ++ticks;
        lapicstate::get().ack();
        schedule();
        break; // Will not be reached

    case INT_PF: {
        // Analyze faulting address and access type.
        uintptr_t addr = rdcr2();
        const char* operation = (regs->reg_errcode & PTE_W) ? "write" : "read";
        const char* problem = (regs->reg_errcode & PTE_P) ? "protection" : "missing page";

        if (!(regs->reg_errcode & PTE_U)) {
            proc_panic(current, "Kernel page fault on %p (%s %s, rip=%p)!\n",
                       addr, operation, problem, regs->reg_rip);
        }
        error_printf(CPOS(24, 0), COLOR_ERROR,
                     "PAGE FAULT on %p (pid %d, %s %s, rip=%p)!\n",
                     addr, current->pid, operation, problem, regs->reg_rip);
        log_print_backtrace(current);
        current->state = P_FAULTED;
        break;
    }

    default:
        proc_panic(current, "Unhandled exception %d (rip=%p)!\n",
                   regs->reg_intno, regs->reg_rip);
    }

    // Decide next action based on process state
    if (current->state == P_RUNNABLE) {
        run(current);
    } else {
        schedule();
    }
}

// syscall(regs)
//    Handle a system call initiated by a `syscall` instruction.

uintptr_t syscall(regstate* regs) {
    // Save current register state
    current->regs = *regs;
    regs = &current->regs;

    // Log syscall details
    log_printf("Process %d: Syscall %ld at RIP %p\n",
               current->pid, regs->reg_rax, regs->reg_rip);

    // Show the current cursor location and memory state
    console_show_cursor(cursorpos);
    memshow();

    // If Control-C was typed, exit the virtual machine.
    check_keyboard();

    // Handle the system call
    switch (regs->reg_rax) {

    case SYSCALL_PANIC:
        user_panic(current);
        break; // Will not be reached

    case SYSCALL_GETPID:
        return current->pid;

    case SYSCALL_YIELD:
        current->regs.reg_rax = 0;
        schedule(); // Does not return

    case SYSCALL_PAGE_ALLOC:
        return syscall_page_alloc(current->regs.reg_rdi);

    case SYSCALL_FORK:
        return syscall_fork();

    case SYSCALL_EXIT:
        sys_exit();
        schedule(); // Should not be reached

    default:
        proc_panic(current, "Unhandled system call %ld (pid=%d, rip=%p)!\n",
                   regs->reg_rax, current->pid, regs->reg_rip);
    }

    panic("Syscall handler should not return here!\n");
}

// syscall_page_alloc(addr)
//    Handles the SYSCALL_PAGE_ALLOC system call.

int syscall_page_alloc(uintptr_t addr) {
    // Ensure the address is page-aligned and within user space bounds
    if (addr % PAGESIZE != 0 || addr < PROC_START_ADDR || addr >= MEMSIZE_VIRTUAL) {
        return -1; // Invalid address
    }

    // Allocate a new physical page
    void* new_page = kalloc(PAGESIZE);
    if (!new_page) {
        return -1; // Allocation failed
    }

    // Initialize with zero
    memset(new_page, 0, PAGESIZE);

    // Map the new page into the current process's page table
    int r = vmiter(current->pagetable, addr).try_map((uintptr_t)new_page, PTE_P | PTE_W | PTE_U);
    if (r != 0) {
        kfree(new_page);
        return -1; // Mapping failed
    }

    return 0; // Success
}

// schedule()
//    Pick the next process to run and then run it.
//    If there are no runnable processes, spins forever.

void schedule() {
    static pid_t last_pid = 0;

    while (true) {
        // Iterate through all possible PIDs starting from last_pid + 1
        for (int i = 0; i < PID_MAX; ++i) {
            last_pid = (last_pid + 1) % PID_MAX;
            proc* p = &ptable[last_pid];

            if (p->state == P_RUNNABLE) {
                run(p);
            }
        }

        // If no runnable process was found, spin
        // Optionally, implement a low-power wait or halt here

        // Periodically display memory state
        if (ticks % (HZ / 4) == 0) { // Every 0.25 seconds
            memshow();
        }

        // Handle keyboard interrupts
        check_keyboard();
    }

    // Spin indefinitely if no runnable processes
    while (true) {
        // Optionally, enter a low-power state or halt
    }
}

// run(p)
//    Run process `p`. This involves setting `current = p` and calling
//    `exception_return` to restore its page table and registers.

void run(proc* p) {
    assert(p->state == P_RUNNABLE);
    current = p;

    // Validate process state
    check_process_registers(p);
    check_pagetable(p->pagetable);

    // Transition to user mode
    exception_return(p);

    // Infinite loop as a safeguard
    while (true) {}
}

// memshow()
//    Draw a picture of memory (physical and virtual) on the CGA console.
//    Switches to a new process's virtual memory map every 0.25 sec.
//    Uses `console_memviewer()`, a function defined in `k-memviewer.cc`.

void memshow() {
    static unsigned last_ticks = 0;
    static int showing = 1; // Start from PID 1

    // Switch to a new process every 0.25 sec
    if (ticks - last_ticks >= HZ / 4) { // 0.25 seconds
        last_ticks = ticks;
        showing = (showing + 1) % PID_MAX;
    }

    proc* p = nullptr;
    if (ptable[showing].state != P_FREE && ptable[showing].pagetable) {
        p = &ptable[showing];
    } else {
        // Find the next valid process
        for (int i = 0; i < PID_MAX; ++i) {
            showing = (showing + 1) % PID_MAX;
            if (ptable[showing].state != P_FREE && ptable[showing].pagetable) {
                p = &ptable[showing];
                break;
            }
        }
    }

    console_memviewer(p);
    if (!p) {
        console_printf(CPOS(10, 26), 0x0F00,
                       "   VIRTUAL ADDRESS SPACE\n"
                       "                          [All processes have exited]\n"
                       "\n\n\n\n\n\n\n\n\n\n\n");
    }
}

// sys_exit()
//    Handles the exit system call by cleaning up the current process.

void sys_exit() {
    // Clean up the current process's page table
    cleanup_pagetable(current->pagetable);

    // Mark the process as free
    current->state = P_FREE;

    // Schedule the next runnable process
    schedule(); // This will switch to another process
}

// cleanup_pagetable(pagetable)
//    Frees all pages mapped in the given page table.

void cleanup_pagetable(x86_64_pagetable* pagetable) {
    // Iterate through all user-space mappings
    for (vmiter it(pagetable, PROC_START_ADDR); it.va() < MEMSIZE_VIRTUAL; it += PAGESIZE) {
        if (it.present() && it.user() && it.va() != CONSOLE_ADDR) {
            kfree((void*)it.pa());
        }
    }

    // Iterate and free lower-level page tables
    for (ptiter pt_it(pagetable); pt_it.va() < MEMSIZE_VIRTUAL; pt_it.next()) {
        if (pt_it.kptr()) {
            kfree(pt_it.kptr());
        }
    }

    // Free the top-level page table
    kfree(pagetable);
}
