// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_THREADPOOL_H_
#define KINGDB_THREADPOOL_H_

#include "util/debug.h"
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>


namespace kdb {

class Task {
 public:
  Task():stop_requested_(false) {}
  virtual ~Task() {}
  virtual void RunInLock(std::thread::id tid) = 0;
  virtual void Run(std::thread::id tid, uint64_t id) = 0;
  bool IsStopRequested() { return stop_requested_; }
  void Stop() { stop_requested_ = true; }
  bool stop_requested_;
};


class ThreadPool {
 // TODO: What if too many items are incoming? add_task()
 //       must return an error beyond a certain limit, or timeout
 // TODO: What if a run() method throws an exception? => force it to be noexcept?
 // TODO: Impose limit on number of items in queue -- for thread pool over
 //       sockets, the queue should be of size 0
 // TODO: Verify that the Stop() method on the tasks makes the workers stop as
 //       expected.
 // TODO: Protect accesses to tid_to_id_ and tid_to_task_ with mutexes
 public:
  int num_threads_; 
  std::queue<Task*> queue_;
  std::condition_variable cv_;
  std::mutex mutex_;
  std::vector<std::thread> threads_;
  std::map<std::thread::id, uint64_t> tid_to_id_;
  std::map<std::thread::id, Task*> tid_to_task_;
  uint64_t seq_id;
  bool stop_requested_;

  ThreadPool(int num_threads) {
    num_threads_ = num_threads;
    seq_id = 0;
    stop_requested_ = false;
  }

  ~ThreadPool() {
  }

  void ProcessingLoop() {
    while (!IsStopRequested()) {
      std::unique_lock<std::mutex> lock(mutex_);
      if (queue_.empty()) {
        cv_.wait(lock);
        if (IsStopRequested()) continue;
      }
      auto task = queue_.front();
      if (task == nullptr) {
        queue_.pop(); // calls 'delete task'
        continue;
      }
      auto tid = std::this_thread::get_id();
      auto it_find = tid_to_id_.find(tid);
      uint64_t id = 0;
      if (it_find == tid_to_id_.end()) id = seq_id++;
      tid_to_id_[tid] = id;
      tid_to_task_[tid] = task;
      task->RunInLock(tid);
      lock.unlock();
      task->Run(tid, id);

      mutex_.lock();
      tid_to_task_[tid] = nullptr; // prevents erase() from calling delete on the task
      tid_to_task_.erase(tid);
      queue_.pop(); // calls 'delete task'
      mutex_.unlock();
    }
  }

  void AddTask(Task* task) {
      std::unique_lock<std::mutex> lock(mutex_);
      queue_.push(task);
      cv_.notify_one();
  }

  void Start() {
    // NOTE: Should each thread run the loop, or should the loop be running in a
    //       main thread that is then dispatching work by notifying other
    //       threads?
    for (auto i = 0; i < num_threads_; i++) {
      threads_.push_back(std::thread(&ThreadPool::ProcessingLoop, this));
    }
  }

  void Stop() {
    stop_requested_ = true;
    mutex_.lock();
    for (auto& tid_task: tid_to_task_) {
      Task* task = tid_task.second;
      task->Stop();
    }
    mutex_.unlock();
    cv_.notify_all();
    for (auto& t: threads_) {
      t.join();
    }
  }

  void BlockUntilAllTasksHaveCompleted() {
    while (queue_.size() > 0) { // TODO: protect accesses to queue_ with a mutex
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    Stop();
  }

  bool IsStopRequested() {
    return stop_requested_;
  }


};

}

#endif // KINGDB_THREADPOOL_H_
