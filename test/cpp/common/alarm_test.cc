//
//
// Copyright 2015 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

#include <grpcpp/alarm.h>
#include <grpcpp/completion_queue.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "src/core/lib/iomgr/exec_ctx.h"
#include "src/core/lib/iomgr/port.h"
#include "src/core/lib/surface/completion_queue.h"
#include "src/core/util/notification.h"
#include "test/core/test_util/test_config.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"

#ifdef GRPC_LINUX_EVENTFD
#include <errno.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#endif

namespace grpc {
namespace {

TEST(AlarmTest, RegularExpiry) {
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  Alarm alarm;
  alarm.Set(&cq, grpc_timeout_seconds_to_deadline(1), junk);

  void* output_tag;
  bool ok;
  const CompletionQueue::NextStatus status =
      cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(10));

  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_TRUE(ok);
  EXPECT_EQ(junk, output_tag);
}

TEST(AlarmTest, RegularExpiryMultiSet) {
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  Alarm alarm;

  for (int i = 0; i < 3; i++) {
    alarm.Set(&cq, grpc_timeout_seconds_to_deadline(1), junk);

    void* output_tag;
    bool ok;
    const CompletionQueue::NextStatus status =
        cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(10));

    EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
    EXPECT_TRUE(ok);
    EXPECT_EQ(junk, output_tag);
  }
}

TEST(AlarmTest, RegularExpiryMultiSetMultiCQ) {
  void* junk = reinterpret_cast<void*>(1618033);
  Alarm alarm;

  for (int i = 0; i < 3; i++) {
    CompletionQueue cq;
    alarm.Set(&cq, grpc_timeout_seconds_to_deadline(1), junk);

    void* output_tag;
    bool ok;
    const CompletionQueue::NextStatus status =
        cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(10));

    EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
    EXPECT_TRUE(ok);
    EXPECT_EQ(junk, output_tag);
  }
}

struct Completion {
  bool completed = false;
  std::mutex mu;
  std::condition_variable cv;
};

TEST(AlarmTest, CallbackRegularExpiry) {
  Alarm alarm;

  auto c = std::make_shared<Completion>();
  alarm.Set(std::chrono::system_clock::now() + std::chrono::seconds(1),
            [c](bool ok) {
              EXPECT_TRUE(ok);
              std::lock_guard<std::mutex> l(c->mu);
              c->completed = true;
              c->cv.notify_one();
            });

  std::unique_lock<std::mutex> l(c->mu);
  EXPECT_TRUE(c->cv.wait_until(
      l, std::chrono::system_clock::now() + std::chrono::seconds(10),
      [c] { return c->completed; }));
}

TEST(AlarmTest, CallbackZeroExpiry) {
  Alarm alarm;

  auto c = std::make_shared<Completion>();
  alarm.Set(grpc_timeout_seconds_to_deadline(0), [c](bool ok) {
    EXPECT_TRUE(ok);
    std::lock_guard<std::mutex> l(c->mu);
    c->completed = true;
    c->cv.notify_one();
  });

  std::unique_lock<std::mutex> l(c->mu);
  EXPECT_TRUE(c->cv.wait_until(
      l, std::chrono::system_clock::now() + std::chrono::seconds(10),
      [c] { return c->completed; }));
}

TEST(AlarmTest, CallbackNegativeExpiry) {
  Alarm alarm;

  auto c = std::make_shared<Completion>();
  alarm.Set(std::chrono::system_clock::now() + std::chrono::seconds(-1),
            [c](bool ok) {
              EXPECT_TRUE(ok);
              std::lock_guard<std::mutex> l(c->mu);
              c->completed = true;
              c->cv.notify_one();
            });

  std::unique_lock<std::mutex> l(c->mu);
  EXPECT_TRUE(c->cv.wait_until(
      l, std::chrono::system_clock::now() + std::chrono::seconds(10),
      [c] { return c->completed; }));
}

TEST(AlarmTest, MultithreadedRegularExpiry) {
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  void* output_tag;
  bool ok;
  CompletionQueue::NextStatus status;
  Alarm alarm;

  std::thread t1([&alarm, &cq, &junk] {
    alarm.Set(&cq, grpc_timeout_seconds_to_deadline(1), junk);
  });

  std::thread t2([&cq, &ok, &output_tag, &status] {
    status =
        cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(10));
  });

  t1.join();
  t2.join();
  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_TRUE(ok);
  EXPECT_EQ(junk, output_tag);
}

TEST(AlarmTest, DeprecatedRegularExpiry) {
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  Alarm alarm(&cq, grpc_timeout_seconds_to_deadline(1), junk);

  void* output_tag;
  bool ok;
  const CompletionQueue::NextStatus status =
      cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(10));

  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_TRUE(ok);
  EXPECT_EQ(junk, output_tag);
}

TEST(AlarmTest, MoveConstructor) {
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  Alarm first;
  first.Set(&cq, grpc_timeout_seconds_to_deadline(1), junk);
  Alarm second(std::move(first));
  void* output_tag;
  bool ok;
  const CompletionQueue::NextStatus status =
      cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(10));
  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_TRUE(ok);
  EXPECT_EQ(junk, output_tag);
}

TEST(AlarmTest, MoveAssignment) {
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  Alarm first;
  first.Set(&cq, grpc_timeout_seconds_to_deadline(1), junk);
  Alarm second(std::move(first));
  first = std::move(second);

  void* output_tag;
  bool ok;
  const CompletionQueue::NextStatus status =
      cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(10));

  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_TRUE(ok);
  EXPECT_EQ(junk, output_tag);
}

TEST(AlarmTest, RegularExpiryChrono) {
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  std::chrono::system_clock::time_point one_sec_deadline =
      std::chrono::system_clock::now() + std::chrono::seconds(1);
  Alarm alarm;
  alarm.Set(&cq, one_sec_deadline, junk);

  void* output_tag;
  bool ok;
  const CompletionQueue::NextStatus status =
      cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(10));

  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_TRUE(ok);
  EXPECT_EQ(junk, output_tag);
}

TEST(AlarmTest, ZeroExpiry) {
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  Alarm alarm;
  alarm.Set(&cq, grpc_timeout_seconds_to_deadline(0), junk);

  void* output_tag;
  bool ok;
  const CompletionQueue::NextStatus status =
      cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(1));

  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_TRUE(ok);
  EXPECT_EQ(junk, output_tag);
}

TEST(AlarmTest, NegativeExpiry) {
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  Alarm alarm;
  alarm.Set(&cq, grpc_timeout_seconds_to_deadline(-1), junk);

  void* output_tag;
  bool ok;
  const CompletionQueue::NextStatus status =
      cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(1));

  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_TRUE(ok);
  EXPECT_EQ(junk, output_tag);
}

// Infinite past or unix epoch should fire immediately.
TEST(AlarmTest, InfPastExpiry) {
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  Alarm alarm;
  alarm.Set(&cq, gpr_inf_past(GPR_CLOCK_REALTIME), junk);

  void* output_tag;
  bool ok;
  CompletionQueue::NextStatus status =
      cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(10));

  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_TRUE(ok);
  EXPECT_EQ(junk, output_tag);

  alarm.Set(&cq, std::chrono::system_clock::time_point(), junk);
  status = cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(10));

  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_TRUE(ok);
  EXPECT_EQ(junk, output_tag);
}

TEST(AlarmTest, Cancellation) {
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  Alarm alarm;
  alarm.Set(&cq, grpc_timeout_seconds_to_deadline(10), junk);
  alarm.Cancel();

  void* output_tag;
  bool ok;
  const CompletionQueue::NextStatus status =
      cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(1));

  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_FALSE(ok);
  EXPECT_EQ(junk, output_tag);
}

TEST(AlarmTest, CancellationMultiSet) {
  // Tests the cancellation and re-Set paths together.
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  Alarm alarm;
  // First iteration
  alarm.Set(&cq, grpc_timeout_seconds_to_deadline(5), junk);
  alarm.Cancel();
  void* output_tag;
  bool ok;
  CompletionQueue::NextStatus status =
      cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(10));
  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_FALSE(ok);
  EXPECT_EQ(junk, output_tag);
  // Second iteration
  alarm.Set(&cq, grpc_timeout_seconds_to_deadline(5), junk);
  alarm.Cancel();
  status = cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(10));
  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_FALSE(ok);
  EXPECT_EQ(junk, output_tag);
}

TEST(AlarmTest, CallbackCancellation) {
  Alarm alarm;

  auto c = std::make_shared<Completion>();
  alarm.Set(std::chrono::system_clock::now() + std::chrono::seconds(10),
            [c](bool ok) {
              EXPECT_FALSE(ok);
              std::lock_guard<std::mutex> l(c->mu);
              c->completed = true;
              c->cv.notify_one();
            });
  alarm.Cancel();

  std::unique_lock<std::mutex> l(c->mu);
  EXPECT_TRUE(c->cv.wait_until(
      l, std::chrono::system_clock::now() + std::chrono::seconds(1),
      [c] { return c->completed; }));
}

TEST(AlarmTest, CallbackCancellationMultiSet) {
  // Tests the cancellation and re-Set paths.
  Alarm alarm;
  // First iteration
  {
    grpc_core::Notification notification;
    alarm.Set(std::chrono::system_clock::now() + std::chrono::seconds(10),
              [&notification](bool ok) {
                EXPECT_FALSE(ok);
                notification.Notify();
              });
    alarm.Cancel();
    notification.WaitForNotification();
  }
  // First iteration
  {
    grpc_core::Notification notification;
    alarm.Set(std::chrono::system_clock::now() + std::chrono::seconds(10),
              [&notification](bool ok) {
                EXPECT_FALSE(ok);
                notification.Notify();
              });
    alarm.Cancel();
    notification.WaitForNotification();
  }
}

TEST(AlarmTest, CallbackCancellationLocked) {
  Alarm alarm;

  auto c = std::make_shared<Completion>();
  alarm.Set(std::chrono::system_clock::now() + std::chrono::seconds(10),
            [c](bool ok) {
              EXPECT_FALSE(ok);
              std::lock_guard<std::mutex> l(c->mu);
              c->completed = true;
              c->cv.notify_one();
            });
  std::unique_lock<std::mutex> l(c->mu);
  alarm.Cancel();

  EXPECT_TRUE(c->cv.wait_until(
      l, std::chrono::system_clock::now() + std::chrono::seconds(1),
      [c] { return c->completed; }));
}

TEST(AlarmTest, SetDestruction) {
  CompletionQueue cq;
  void* junk = reinterpret_cast<void*>(1618033);
  {
    Alarm alarm;
    alarm.Set(&cq, grpc_timeout_seconds_to_deadline(10), junk);
  }

  void* output_tag;
  bool ok;
  const CompletionQueue::NextStatus status =
      cq.AsyncNext(&output_tag, &ok, grpc_timeout_seconds_to_deadline(1));

  EXPECT_EQ(status, CompletionQueue::GOT_EVENT);
  EXPECT_FALSE(ok);
  EXPECT_EQ(junk, output_tag);
}

TEST(AlarmTest, CallbackSetDestruction) {
  auto c = std::make_shared<Completion>();
  {
    Alarm alarm;
    alarm.Set(std::chrono::system_clock::now() + std::chrono::seconds(10),
              [c](bool ok) {
                EXPECT_FALSE(ok);
                std::lock_guard<std::mutex> l(c->mu);
                c->completed = true;
                c->cv.notify_one();
              });
  }

  std::unique_lock<std::mutex> l(c->mu);
  EXPECT_TRUE(c->cv.wait_until(
      l, std::chrono::system_clock::now() + std::chrono::seconds(1),
      [c] { return c->completed; }));
}

TEST(AlarmTest, UnsetDestruction) {
  CompletionQueue cq;
  Alarm alarm;
}

TEST(AlarmTest, CallbackSetInCallback) {
  Completion c;
  std::mutex alarm_mu;
  Alarm alarm;
  {
    std::lock_guard<std::mutex> l(alarm_mu);
    alarm.Set(std::chrono::system_clock::now() + std::chrono::seconds(1),
              [&](bool ok) {
                EXPECT_TRUE(ok);
                std::lock_guard<std::mutex> l(alarm_mu);
                alarm.Set(
                    std::chrono::system_clock::now() + std::chrono::seconds(1),
                    [&](bool ok) {
                      EXPECT_TRUE(ok);
                      std::lock_guard<std::mutex> l(c.mu);
                      c.completed = true;
                      c.cv.notify_one();
                    });
              });
  }
  std::unique_lock<std::mutex> l(c.mu);
  c.cv.wait(l, [&] { return c.completed; });
}

#ifdef GRPC_LINUX_EVENTFD
TEST(AlarmTest, ReleaseEventFdMultiplexing) {
  CompletionQueue cq1;
  CompletionQueue cq2;
  const int efd1 = cq1.ReleaseEventFd();
  const int efd2 = cq2.ReleaseEventFd();
  ASSERT_GE(efd1, 0);
  ASSERT_GE(efd2, 0);
  EXPECT_EQ(cq1.ReleaseEventFd(), -1);

  const int custom_efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  ASSERT_GE(custom_efd, 0);

  const int epfd = epoll_create1(EPOLL_CLOEXEC);
  ASSERT_GE(epfd, 0);

  auto add_to_epoll = [epfd](int fd) {
    struct epoll_event ev = {};
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    ASSERT_EQ(epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev), 0);
  };
  add_to_epoll(efd1);
  add_to_epoll(efd2);
  add_to_epoll(custom_efd);

  void* tag1a = reinterpret_cast<void*>(101);
  void* tag1b = reinterpret_cast<void*>(102);
  void* tag2 = reinterpret_cast<void*>(202);
  Alarm alarm1a;
  Alarm alarm1b;
  Alarm alarm2;
  alarm1a.Set(&cq1, gpr_inf_past(GPR_CLOCK_REALTIME), tag1a);
  alarm1b.Set(&cq1, gpr_inf_past(GPR_CLOCK_REALTIME), tag1b);
  alarm2.Set(&cq2,
             std::chrono::system_clock::now() + std::chrono::milliseconds(50),
             tag2);
  ASSERT_EQ(eventfd_write(custom_efd, 1), 0);

  bool got_tag1a = false;
  bool got_tag1b = false;
  bool got_tag2 = false;
  bool got_custom = false;
  while (!got_tag1a || !got_tag1b || !got_tag2 || !got_custom) {
    struct epoll_event events[3];
    int n = epoll_wait(epfd, events, 3, 5000);
    if (n < 0 && errno == EINTR) continue;
    ASSERT_GT(n, 0);
    for (int i = 0; i < n; i++) {
      const int fd = events[i].data.fd;
      eventfd_t val = 0;
      ASSERT_EQ(eventfd_read(fd, &val), 0);
      if (fd == custom_efd) {
        got_custom = true;
      } else {
        CompletionQueue* cq = (fd == efd1) ? &cq1 : &cq2;
        for (;;) {
          void* output_tag = nullptr;
          bool ok = false;
          const CompletionQueue::NextStatus status =
              cq->AsyncNext(&output_tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
          if (status == CompletionQueue::TIMEOUT) {
            break;
          }
          ASSERT_EQ(status, CompletionQueue::GOT_EVENT);
          EXPECT_TRUE(ok);
          if (fd == efd1) {
            if (output_tag == tag1a) {
              got_tag1a = true;
            } else if (output_tag == tag1b) {
              got_tag1b = true;
            } else {
              FAIL() << "Unexpected tag on cq1: " << output_tag;
            }
          } else {
            EXPECT_EQ(output_tag, tag2);
            got_tag2 = true;
          }
        }
      }
    }
  }

  // Shutdown both CompletionQueues and verify epoll_wait wakes up for SHUTDOWN.
  cq1.Shutdown();
  cq2.Shutdown();
  bool cq1_shutdown = false;
  bool cq2_shutdown = false;
  while (!cq1_shutdown || !cq2_shutdown) {
    struct epoll_event events[2];
    int n = epoll_wait(epfd, events, 2, 5000);
    if (n < 0 && errno == EINTR) continue;
    ASSERT_GT(n, 0);
    for (int i = 0; i < n; i++) {
      const int fd = events[i].data.fd;
      eventfd_t val = 0;
      ASSERT_EQ(eventfd_read(fd, &val), 0);
      CompletionQueue* cq = (fd == efd1) ? &cq1 : &cq2;
      void* output_tag = nullptr;
      bool ok = false;
      const CompletionQueue::NextStatus status =
          cq->AsyncNext(&output_tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
      ASSERT_EQ(status, CompletionQueue::SHUTDOWN);
      if (fd == efd1) {
        cq1_shutdown = true;
      } else {
        cq2_shutdown = true;
      }
    }
  }

  close(epfd);
  close(custom_efd);
  close(efd1);
  close(efd2);
}

TEST(AlarmTest, ReleaseEventFdWithAsyncNextBatch) {
  CompletionQueue cq;
  const int efd = cq.ReleaseEventFd();
  ASSERT_GE(efd, 0);

  const int epfd = epoll_create1(EPOLL_CLOEXEC);
  ASSERT_GE(epfd, 0);
  struct epoll_event ev = {};
  ev.events = EPOLLIN;
  ev.data.fd = efd;
  ASSERT_EQ(epoll_ctl(epfd, EPOLL_CTL_ADD, efd, &ev), 0);

  constexpr int kNumAlarms = 25;
  constexpr size_t kBatchSize = 16;
  Alarm alarms[kNumAlarms];
  for (int i = 0; i < kNumAlarms; i++) {
    alarms[i].Set(&cq, gpr_inf_past(GPR_CLOCK_REALTIME),
                  reinterpret_cast<void*>(static_cast<intptr_t>(i + 1)));
  }

  int drained = 0;
  while (drained < kNumAlarms) {
    struct epoll_event out_ev = {};
    int n = epoll_wait(epfd, &out_ev, 1, 5000);
    if (n < 0 && errno == EINTR) continue;
    ASSERT_EQ(n, 1);
    eventfd_t val = 0;
    ASSERT_EQ(eventfd_read(efd, &val), 0);

    for (;;) {
      void* tags[kBatchSize] = {};
      bool oks[kBatchSize] = {};
      size_t count = 0;
      const CompletionQueue::NextStatus status = cq.AsyncNext(
          tags, oks, kBatchSize, &count, gpr_time_0(GPR_CLOCK_REALTIME));
      if (status == CompletionQueue::TIMEOUT) {
        EXPECT_EQ(count, 0u);
        break;
      }
      ASSERT_EQ(status, CompletionQueue::GOT_EVENT);
      ASSERT_GE(count, 1u);
      ASSERT_LE(count, kBatchSize);
      for (size_t i = 0; i < count; i++) {
        EXPECT_NE(tags[i], nullptr);
        EXPECT_TRUE(oks[i]);
        drained++;
      }
    }
  }
  EXPECT_EQ(drained, kNumAlarms);

  cq.Shutdown();
  struct epoll_event out_ev = {};
  int n = 0;
  do {
    n = epoll_wait(epfd, &out_ev, 1, 5000);
  } while (n < 0 && errno == EINTR);
  ASSERT_EQ(n, 1);
  eventfd_t val = 0;
  ASSERT_EQ(eventfd_read(efd, &val), 0);

  void* tags[kBatchSize] = {};
  bool oks[kBatchSize] = {};
  size_t count = 99;
  EXPECT_EQ(cq.AsyncNext(tags, oks, kBatchSize, &count,
                         gpr_time_0(GPR_CLOCK_REALTIME)),
            CompletionQueue::SHUTDOWN);
  EXPECT_EQ(count, 0u);

  close(epfd);
  close(efd);
}
#else   // GRPC_LINUX_EVENTFD
TEST(AlarmTest, ReleaseEventFdUnsupported) {
  CompletionQueue cq;
  EXPECT_EQ(cq.ReleaseEventFd(), -1);
  cq.Shutdown();
  void* tag = nullptr;
  bool ok = false;
  EXPECT_EQ(cq.AsyncNext(&tag, &ok, gpr_inf_future(GPR_CLOCK_REALTIME)),
            CompletionQueue::SHUTDOWN);
}
#endif  // GRPC_LINUX_EVENTFD

class TestCqTag final : public internal::CompletionQueueTag {
 public:
  TestCqTag(void* user_tag, bool is_user_visible, int* finalized_count)
      : user_tag_(user_tag),
        is_user_visible_(is_user_visible),
        finalized_count_(finalized_count) {}

  bool FinalizeResult(void** tag, bool* /*status*/) override {
    if (finalized_count_ != nullptr) {
      (*finalized_count_)++;
    }
    *tag = user_tag_;
    return is_user_visible_;
  }

  grpc_cq_completion* completion() { return &completion_; }

 private:
  void* user_tag_;
  bool is_user_visible_;
  int* finalized_count_;
  grpc_cq_completion completion_;
};

void EnqueueTestCqTag(CompletionQueue* cq, TestCqTag* tag, bool ok) {
  grpc_core::ExecCtx exec_ctx;
  ASSERT_TRUE(grpc_cq_begin_op(cq->cq(), tag));
  grpc_cq_end_op(
      cq->cq(), tag, ok ? absl::OkStatus() : absl::CancelledError("cancelled"),
      [](void* /*arg*/, grpc_cq_completion* /*c*/) {}, nullptr,
      tag->completion());
}

TEST(AlarmTest, AsyncNextBatch) {
  CompletionQueue cq;
  void* tags[16] = {};
  bool oks[16] = {};
  size_t count = 99;

  // 1. Empty queue timeout sets count to 0.
  EXPECT_EQ(cq.AsyncNext(tags, oks, 16, &count, gpr_time_0(GPR_CLOCK_REALTIME)),
            CompletionQueue::TIMEOUT);
  EXPECT_EQ(count, 0u);

  // 2. max_events == 0 returns TIMEOUT with count == 0 without consuming
  // events.
  Alarm alarm_expired;
  Alarm alarm_cancelled;
  void* tag_exp = reinterpret_cast<void*>(1001);
  void* tag_can = reinterpret_cast<void*>(1002);
  alarm_expired.Set(&cq, gpr_inf_past(GPR_CLOCK_REALTIME), tag_exp);
  alarm_cancelled.Set(&cq, grpc_timeout_seconds_to_deadline(60), tag_can);
  alarm_cancelled.Cancel();

  count = 99;
  EXPECT_EQ(cq.AsyncNext(nullptr, nullptr, 0, &count,
                         grpc_timeout_seconds_to_deadline(5)),
            CompletionQueue::TIMEOUT);
  EXPECT_EQ(count, 0u);

  // Drain the 2 alarms using batched AsyncNext.
  bool saw_exp = false;
  bool saw_can = false;
  while (!saw_exp || !saw_can) {
    count = 0;
    ASSERT_EQ(cq.AsyncNext(tags, oks, 16, &count,
                           grpc_timeout_seconds_to_deadline(5)),
              CompletionQueue::GOT_EVENT);
    ASSERT_GE(count, 1u);
    ASSERT_LE(count, 2u);
    for (size_t i = 0; i < count; i++) {
      if (tags[i] == tag_exp) {
        EXPECT_TRUE(oks[i]);
        saw_exp = true;
      } else if (tags[i] == tag_can) {
        EXPECT_FALSE(oks[i]);
        saw_can = true;
      } else {
        FAIL() << "Unexpected tag: " << tags[i];
      }
    }
  }

  // 3. Large batch capacity (> 64 inline buffer) and multi-batch draining.
  constexpr size_t kLargeTotal = 75;
  constexpr size_t kLargeCapacity = 100;
  std::vector<std::unique_ptr<TestCqTag>> large_tags;
  large_tags.reserve(kLargeTotal);
  int finalized = 0;
  for (size_t i = 0; i < kLargeTotal; i++) {
    large_tags.push_back(std::make_unique<TestCqTag>(
        reinterpret_cast<void*>(i + 1), /*is_user_visible=*/true, &finalized));
    EnqueueTestCqTag(&cq, large_tags.back().get(), (i % 3) != 0);
  }

  void* large_out_tags[kLargeCapacity] = {};
  bool large_out_oks[kLargeCapacity] = {};
  count = 0;
  ASSERT_EQ(cq.AsyncNext(large_out_tags, large_out_oks, kLargeCapacity, &count,
                         gpr_time_0(GPR_CLOCK_REALTIME)),
            CompletionQueue::GOT_EVENT);
  ASSERT_EQ(count, kLargeTotal);
  EXPECT_EQ(finalized, static_cast<int>(kLargeTotal));
  for (size_t i = 0; i < kLargeTotal; i++) {
    EXPECT_EQ(large_out_tags[i], reinterpret_cast<void*>(i + 1));
    EXPECT_EQ(large_out_oks[i], (i % 3) != 0);
  }

  // 4. Shutdown returns SHUTDOWN with count == 0.
  cq.Shutdown();
  count = 99;
  EXPECT_EQ(
      cq.AsyncNext(tags, oks, 16, &count, grpc_timeout_seconds_to_deadline(5)),
      CompletionQueue::SHUTDOWN);
  EXPECT_EQ(count, 0u);
}

TEST(AlarmTest, AsyncNextBatchInternalTagFiltering) {
  CompletionQueue cq;

  // 1. Mixed batch: 3 internal tags (FinalizeResult -> false) interleaved with
  //    2 user-visible tags (FinalizeResult -> true). Verify compaction into
  //    tags[0..1] and oks[0..1], and verify untouched slots retain sentinels.
  int finalized = 0;
  void* const kSentinelTag = reinterpret_cast<void*>(0xdeadbeef);
  TestCqTag t1(reinterpret_cast<void*>(11), /*is_user_visible=*/false,
               &finalized);
  TestCqTag t2(reinterpret_cast<void*>(22), /*is_user_visible=*/true,
               &finalized);
  TestCqTag t3(reinterpret_cast<void*>(33), /*is_user_visible=*/false,
               &finalized);
  TestCqTag t4(reinterpret_cast<void*>(44), /*is_user_visible=*/true,
               &finalized);
  TestCqTag t5(reinterpret_cast<void*>(55), /*is_user_visible=*/false,
               &finalized);

  EnqueueTestCqTag(&cq, &t1, true);
  EnqueueTestCqTag(&cq, &t2, true);
  EnqueueTestCqTag(&cq, &t3, false);
  EnqueueTestCqTag(&cq, &t4, false);
  EnqueueTestCqTag(&cq, &t5, true);

  void* tags[8];
  bool oks[8];
  for (int i = 0; i < 8; i++) {
    tags[i] = kSentinelTag;
    oks[i] = (i % 2) == 0;
  }
  size_t count = 0;
  ASSERT_EQ(cq.AsyncNext(tags, oks, 8, &count, gpr_time_0(GPR_CLOCK_REALTIME)),
            CompletionQueue::GOT_EVENT);
  ASSERT_EQ(count, 2u);
  EXPECT_EQ(finalized, 5);
  EXPECT_EQ(tags[0], reinterpret_cast<void*>(22));
  EXPECT_TRUE(oks[0]);
  EXPECT_EQ(tags[1], reinterpret_cast<void*>(44));
  EXPECT_FALSE(oks[1]);
  // Slots beyond count must not be modified by swallowed internal tags.
  for (int i = 2; i < 8; i++) {
    EXPECT_EQ(tags[i], kSentinelTag);
    EXPECT_EQ(oks[i], (i % 2) == 0);
  }

  // 2. All-internal first Core batch followed by user-visible tags in the
  //    second Core batch: with max_events = 3, the first 3 tags are internal,
  //    so AsyncNext must loop and return the 4th (user-visible) tag.
  finalized = 0;
  TestCqTag i1(reinterpret_cast<void*>(101), false, &finalized);
  TestCqTag i2(reinterpret_cast<void*>(102), false, &finalized);
  TestCqTag i3(reinterpret_cast<void*>(103), false, &finalized);
  TestCqTag u1(reinterpret_cast<void*>(201), true, &finalized);
  EnqueueTestCqTag(&cq, &i1, true);
  EnqueueTestCqTag(&cq, &i2, true);
  EnqueueTestCqTag(&cq, &i3, true);
  EnqueueTestCqTag(&cq, &u1, true);

  count = 0;
  ASSERT_EQ(cq.AsyncNext(tags, oks, 3, &count, gpr_time_0(GPR_CLOCK_REALTIME)),
            CompletionQueue::GOT_EVENT);
  ASSERT_EQ(count, 1u);
  EXPECT_EQ(finalized, 4);
  EXPECT_EQ(tags[0], reinterpret_cast<void*>(201));
  EXPECT_TRUE(oks[0]);

  // 3. All-internal batch followed by empty queue returns TIMEOUT (count == 0).
  finalized = 0;
  TestCqTag i4(reinterpret_cast<void*>(301), false, &finalized);
  TestCqTag i5(reinterpret_cast<void*>(302), false, &finalized);
  EnqueueTestCqTag(&cq, &i4, true);
  EnqueueTestCqTag(&cq, &i5, true);

  count = 99;
  ASSERT_EQ(cq.AsyncNext(tags, oks, 4, &count, gpr_time_0(GPR_CLOCK_REALTIME)),
            CompletionQueue::TIMEOUT);
  EXPECT_EQ(count, 0u);
  EXPECT_EQ(finalized, 2);

  // 4. All-internal batch followed by CQ shutdown returns SHUTDOWN (count ==
  //    0).
  finalized = 0;
  TestCqTag i6(reinterpret_cast<void*>(401), false, &finalized);
  EnqueueTestCqTag(&cq, &i6, true);
  cq.Shutdown();

  count = 99;
  ASSERT_EQ(
      cq.AsyncNext(tags, oks, 4, &count, grpc_timeout_seconds_to_deadline(5)),
      CompletionQueue::SHUTDOWN);
  EXPECT_EQ(count, 0u);
  EXPECT_EQ(finalized, 1);
}

}  // namespace
}  // namespace grpc

int main(int argc, char** argv) {
  grpc::testing::TestEnvironment env(&argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
