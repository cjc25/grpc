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

#include "src/core/lib/iomgr/port.h"
#include "src/core/util/notification.h"
#include "test/core/test_util/test_config.h"
#include "gtest/gtest.h"

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

}  // namespace
}  // namespace grpc

int main(int argc, char** argv) {
  grpc::testing::TestEnvironment env(&argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
