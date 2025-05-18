#include "tasksys.h"
#include <algorithm>


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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
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
        TaskID task_graph_id = -1;
        int task_id = -1;
        IRunnable* runnable = nullptr;
        int num_total_tasks = 0;
        
        // Wait for tasks or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            
            // Wait until there are ready tasks or shutdown is signaled
            cv_workers.wait(lock, [this] {
                return !ready_tasks.empty() || shutdown;
            });
            
            // If we're shutting down and no tasks, exit thread
            if (shutdown && ready_tasks.empty()) {
                break;
            }
            
            // Get a task from the ready queue if available
            if (!ready_tasks.empty()) {
                task_graph_id = ready_tasks.front().first;
                task_id = ready_tasks.front().second;
                ready_tasks.pop();
                
                // Get the runnable and task info
                auto& task_info = task_graph[task_graph_id];
                runnable = task_info.runnable;
                num_total_tasks = task_info.num_total_tasks;
            }
        }
        
        // Execute the task if we got one
        if (runnable != nullptr && task_id >= 0) {
            runnable->runTask(task_id, num_total_tasks);
            
            // Update task completion status
            std::unique_lock<std::mutex> lock(queue_mutex);
            
            // Decrement remaining tasks for this bulk launch
            auto& task_info = task_graph[task_graph_id];
            task_info.tasks_remaining--;
            
            // If all tasks in this bulk launch are complete
            if (task_info.tasks_remaining == 0) {
                // Notify any dependent tasks that this bulk task is complete
                make_tasks_ready(task_graph_id);
                
                // Notify the main thread if it's waiting for completion
                cv_main.notify_one();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::make_tasks_ready(TaskID completed_task_id) {
    // Find all tasks that depend on the completed task
    auto& completed_task = task_graph[completed_task_id];
    
    // For each dependent task
    for (TaskID dependent_id : completed_task.dependent_tasks) {
        auto& dependent_task = task_graph[dependent_id];
        
        // Remove this dependency
        auto dep_it = std::find(dependent_task.deps.begin(), dependent_task.deps.end(), completed_task_id);
        if (dep_it != dependent_task.deps.end()) {
            dependent_task.deps.erase(dep_it);
        }
        
        // If all dependencies are satisfied, mark as ready and add tasks to ready queue
        if (dependent_task.deps.empty() && !dependent_task.is_ready) {
            dependent_task.is_ready = true;
            
            // Add all individual tasks from this bulk launch to the ready queue
            for (int i = 0; i < dependent_task.num_total_tasks; i++) {
                ready_tasks.push(std::make_pair(dependent_id, i));
            }
            
            // Notify workers that new tasks are available
            cv_workers.notify_all();
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
    this->shutdown = false;
    this->next_task_id = 1; // Start with task ID 1
    
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
    // Implement run() using runAsyncWithDeps() and sync()
    std::vector<TaskID> noDeps;
    runAsyncWithDeps(runnable, num_total_tasks, noDeps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    // Generate a new task ID for this bulk launch
    TaskID new_task_id = next_task_id.fetch_add(1);
    
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        
        // Create entry for this bulk task launch
        BulkTask new_task;
        new_task.runnable = runnable;
        new_task.num_total_tasks = num_total_tasks;
        new_task.deps = deps;  // Store dependencies
        new_task.tasks_remaining = num_total_tasks;
        new_task.is_ready = deps.empty(); // Ready if no dependencies
        
        // Update the dependency graph by informing dependencies that we depend on them
        for (TaskID dep_id : deps) {
            if (task_graph.find(dep_id) != task_graph.end()) {
                task_graph[dep_id].dependent_tasks.insert(new_task_id);
            }
        }
        
        // Add to task graph
        task_graph[new_task_id] = new_task;
        
        // If task has no dependencies, add all its sub-tasks to the ready queue
        if (new_task.is_ready) {
            for (int i = 0; i < num_total_tasks; i++) {
                ready_tasks.push(std::make_pair(new_task_id, i));
            }
            
            // Notify workers that new tasks are available
            cv_workers.notify_all();
        }
    }
    
    return new_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(queue_mutex);
    
    // Check if there are any unfinished tasks in the graph
    bool all_complete = true;
    for (auto& task_pair : task_graph) {
        if (task_pair.second.tasks_remaining > 0) {
            all_complete = false;
            break;
        }
    }
    
    // If all tasks are already complete, return immediately
    if (all_complete) {
        return;
    }
    
    // Wait for all tasks to complete
    cv_main.wait(lock, [this] {
        // Recheck all tasks
        for (auto& task_pair : task_graph) {
            if (task_pair.second.tasks_remaining > 0) {
                return false;
            }
        }
        return true;
    });
}
