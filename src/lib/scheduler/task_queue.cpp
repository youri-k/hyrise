#include "task_queue.hpp"

#include "abstract_task.hpp"

namespace opossum {

TaskQueue::TaskQueue(NodeID node_id) : _node_id(node_id) {}

bool TaskQueue::empty() const { return _num_tasks == 0; }

NodeID TaskQueue::node_id() const { return _node_id; }

void TaskQueue::push(const std::shared_ptr<AbstractTask>& task, uint32_t priority) {
  DebugAssert((priority < NUM_PRIORITY_LEVELS), "Illegal priority level");

  // Someone else was first to enqueue this task? No problem!
  if (!task->try_mark_as_enqueued()) return;

  task->set_node_id(_node_id);
  _queues[priority].push(task);

  _num_tasks++;
}

std::shared_ptr<AbstractTask> TaskQueue::pull() {
  std::shared_ptr<AbstractTask> task;
  for (auto& queue : _queues) {
    if (queue.try_pop(task)) {
      _num_tasks--;
      return task;
    }
  }
  return nullptr;
}

std::shared_ptr<AbstractTask> TaskQueue::steal() {
  std::shared_ptr<AbstractTask> task;
  for (auto& queue : _queues) {
    if (queue.try_pop(task)) {
      if (task->is_stealable()) {
        _num_tasks--;
        return task;
      } else {
        queue.push(task);
      }
    }
  }
  return nullptr;
}

}  // namespace opossum
