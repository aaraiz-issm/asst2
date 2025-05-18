#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // Create threads and distribute tasks using dynamic assignment
    std::vector<std::thread> threads;
    std::atomic<int> next_task(0);
    
    // Worker thread function
    auto worker = [&](int thread_id) {
        while (true) {
            // Get the next task atomically
            int task_id = next_task.fetch_add(1);
            
            // If we've processed all tasks, exit
            if (task_id >= num_total_tasks) {
                break;
            }
            
            // Run the assigned task
            runnable->runTask(task_id, num_total_tasks);
        }
    };
    
    // Create worker threads (at most num_threads, but no more than num_total_tasks)
    int thread_count = std::min(num_threads, num_total_tasks);
    for (int i = 0; i < thread_count; i++) {
        threads.push_back(std::thread(worker, i));
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

void TaskSystemParallelThreadPoolSpinning::worker_thread() {
    while (!shutdown) {
        int task_id = -1;
        IRunnable* runnable = nullptr;
        
        // Check if there are tasks to execute
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            if (!tasks.empty()) {
                // Get the task info
                task_id = tasks.front().second;
                runnable = tasks.front().first;
                tasks.pop();
            }
        }
        
        // If we got a task, execute it
        if (runnable != nullptr && task_id != -1) {
            runnable->runTask(task_id, total_tasks);
            
            // Decrement the task counter
            tasks_remaining--;
        }
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
    this->shutdown = false;
    this->tasks_remaining = 0;
    this->total_tasks = 0;
    
    // Create the thread pool
    for (int i = 0; i < num_threads; i++) {
        thread_pool.push_back(std::thread(&TaskSystemParallelThreadPoolSpinning::worker_thread, this));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        shutdown = true;
    }
    
    // Join all worker threads
    for (auto& thread : thread_pool) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // Set up task information
    total_tasks = num_total_tasks;
    tasks_remaining = num_total_tasks;
    
    // Add all tasks to the queue
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        for (int i = 0; i < num_total_tasks; i++) {
            tasks.push(std::make_pair(runnable, i));
        }
    }
    
    // Wait for all tasks to complete (spin)
    while (tasks_remaining > 0) {
        // Just spin - yielding helps reduce CPU usage while still being responsive
        std::this_thread::yield();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

void TaskSystemParallelThreadPoolSleeping::worker_thread() {
    while (true) {
        int task_id = -1;
        IRunnable* runnable = nullptr;
        
        // Wait for tasks or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            
            // Wait until there are tasks or shutdown is signaled
            cv_workers.wait(lock, [this] {
                return !tasks.empty() || shutdown;
            });
            
            // If we're shutting down and no tasks, exit thread
            if (shutdown && tasks.empty()) {
                break;
            }
            
            // Get the task
            if (!tasks.empty()) {
                task_id = tasks.front().second;
                runnable = tasks.front().first;
                tasks.pop();
            }
        }
        
        // Execute the task if we got one
        if (runnable != nullptr && task_id != -1) {
            runnable->runTask(task_id, total_tasks);
            
            // Decrement task counter and notify if all done
            int remaining = --tasks_remaining;
            if (remaining == 0) {
                cv_main.notify_one();
            }
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
    this->shutdown = false;
    this->tasks_remaining = 0;
    this->total_tasks = 0;
    
    // Create the thread pool
    for (int i = 0; i < num_threads; i++) {
        thread_pool.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::worker_thread, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    // Signal shutdown to all worker threads
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        shutdown = true;
    }
    
    // Wake up all worker threads to check shutdown condition
    cv_workers.notify_all();
    
    // Join all threads
    for (auto& thread : thread_pool) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // Setup task information
    total_tasks = num_total_tasks;
    tasks_remaining = num_total_tasks;
    
    // Add all tasks to the queue
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        for (int i = 0; i < num_total_tasks; i++) {
            tasks.push(std::make_pair(runnable, i));
        }
    }
    
    // Wake up worker threads
    cv_workers.notify_all();
    
    // Wait for all tasks to complete using condition variable
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        cv_main.wait(lock, [this] {
            return tasks_remaining == 0;
        });
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
