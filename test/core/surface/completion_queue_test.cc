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

#include "src/core/lib/surface/completion_queue.h"

#include <grpc/grpc.h>
#include <grpc/support/sync.h>
#include <grpc/support/time.h>
#include <stddef.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "src/core/lib/event_engine/shim.h"
#include "src/core/lib/iomgr/exec_ctx.h"
#include "src/core/lib/iomgr/port.h"
#include "src/core/util/useful.h"
#include "test/core/test_util/test_config.h"
#include "gtest/gtest.h"
#include "absl/log/log.h"
#include "absl/status/status.h"

#ifdef GRPC_LINUX_EVENTFD
#include <errno.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#endif

#define LOG_TEST(x) LOG(INFO) << x

static void* create_test_tag(void) {
  static intptr_t i = 0;
  return reinterpret_cast<void*>(++i);
}

// helper for tests to shutdown correctly and tersely
static void shutdown_and_destroy(grpc_completion_queue* cc) {
  grpc_event ev;
  grpc_completion_queue_shutdown(cc);

  switch (grpc_get_cq_completion_type(cc)) {
    case GRPC_CQ_NEXT: {
      ev = grpc_completion_queue_next(cc, gpr_inf_past(GPR_CLOCK_REALTIME),
                                      nullptr);
      ASSERT_EQ(ev.type, GRPC_QUEUE_SHUTDOWN);
      break;
    }
    case GRPC_CQ_PLUCK: {
      ev = grpc_completion_queue_pluck(
          cc, create_test_tag(), gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
      ASSERT_EQ(ev.type, GRPC_QUEUE_SHUTDOWN);
      break;
    }
    case GRPC_CQ_CALLBACK: {
      // Nothing to do here. The shutdown callback will be invoked when
      // possible.
      break;
    }
    default: {
      LOG(ERROR) << "Unknown completion type";
      break;
    }
  }

  grpc_completion_queue_destroy(cc);
}

// ensure we can create and destroy a completion channel
TEST(GrpcCompletionQueueTest, TestNoOp) {
  grpc_cq_completion_type completion_types[] = {GRPC_CQ_NEXT, GRPC_CQ_PLUCK};
  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  grpc_completion_queue_attributes attr = {};
  LOG_TEST("test_no_op");

  attr.version = 1;
  for (size_t i = 0; i < GPR_ARRAY_SIZE(completion_types); i++) {
    for (size_t j = 0; j < GPR_ARRAY_SIZE(polling_types); j++) {
      attr.cq_completion_type = completion_types[i];
      attr.cq_polling_type = polling_types[j];
      shutdown_and_destroy(grpc_completion_queue_create(
          grpc_completion_queue_factory_lookup(&attr), &attr, nullptr));
    }
  }
}

TEST(GrpcCompletionQueueTest, TestPollsetConversion) {
  if (grpc_event_engine::experimental::UsePollsetAlternative()) {
    GTEST_SKIP_(
        "Pollsets aren't used when EventEngine experiments are enabled.");
  }
  grpc_cq_completion_type completion_types[] = {GRPC_CQ_NEXT, GRPC_CQ_PLUCK};
  grpc_cq_polling_type polling_types[] = {GRPC_CQ_DEFAULT_POLLING,
                                          GRPC_CQ_NON_LISTENING};
  grpc_completion_queue* cq;
  grpc_completion_queue_attributes attr = {};

  LOG_TEST("test_pollset_conversion");

  attr.version = 1;
  for (size_t i = 0; i < GPR_ARRAY_SIZE(completion_types); i++) {
    for (size_t j = 0; j < GPR_ARRAY_SIZE(polling_types); j++) {
      attr.cq_completion_type = completion_types[i];
      attr.cq_polling_type = polling_types[j];
      cq = grpc_completion_queue_create(
          grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);
      ASSERT_NE(grpc_cq_pollset(cq), nullptr);
      shutdown_and_destroy(cq);
    }
  }
}

TEST(GrpcCompletionQueueTest, TestWaitEmpty) {
  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  grpc_completion_queue* cc;
  grpc_completion_queue_attributes attr = {};
  grpc_event event;

  LOG_TEST("test_wait_empty");

  attr.version = 1;
  attr.cq_completion_type = GRPC_CQ_NEXT;
  for (size_t i = 0; i < GPR_ARRAY_SIZE(polling_types); i++) {
    attr.cq_polling_type = polling_types[i];
    cc = grpc_completion_queue_create(
        grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);
    event =
        grpc_completion_queue_next(cc, gpr_now(GPR_CLOCK_REALTIME), nullptr);
    ASSERT_EQ(event.type, GRPC_QUEUE_TIMEOUT);
    shutdown_and_destroy(cc);
  }
}

static void do_nothing_end_completion(void* /*arg*/,
                                      grpc_cq_completion* /*c*/) {}

TEST(GrpcCompletionQueueTest, TestCqEndOp) {
  grpc_event ev;
  grpc_completion_queue* cc;
  grpc_cq_completion completion;
  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  grpc_completion_queue_attributes attr = {};
  void* tag = create_test_tag();

  LOG_TEST("test_cq_end_op");

  attr.version = 1;
  attr.cq_completion_type = GRPC_CQ_NEXT;
  for (size_t i = 0; i < GPR_ARRAY_SIZE(polling_types); i++) {
    grpc_core::ExecCtx exec_ctx;
    attr.cq_polling_type = polling_types[i];
    cc = grpc_completion_queue_create(
        grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);

    ASSERT_TRUE(grpc_cq_begin_op(cc, tag));
    grpc_cq_end_op(cc, tag, absl::OkStatus(), do_nothing_end_completion,
                   nullptr, &completion);

    ev = grpc_completion_queue_next(cc, gpr_inf_past(GPR_CLOCK_REALTIME),
                                    nullptr);
    ASSERT_EQ(ev.type, GRPC_OP_COMPLETE);
    ASSERT_EQ(ev.tag, tag);
    ASSERT_TRUE(ev.success);

    shutdown_and_destroy(cc);
  }
}

TEST(GrpcCompletionQueueTest, TestCqTlsCacheFull) {
  grpc_event ev;
  grpc_completion_queue* cc;
  grpc_cq_completion completion;
  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  grpc_completion_queue_attributes attr = {};
  void* tag = create_test_tag();
  void* res_tag;
  int ok;

  LOG_TEST("test_cq_tls_cache_full");

  attr.version = 1;
  attr.cq_completion_type = GRPC_CQ_NEXT;
  for (size_t i = 0; i < GPR_ARRAY_SIZE(polling_types); i++) {
    grpc_core::ExecCtx exec_ctx;  // Reset exec_ctx
    attr.cq_polling_type = polling_types[i];
    cc = grpc_completion_queue_create(
        grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);

    grpc_completion_queue_thread_local_cache_init(cc);
    ASSERT_TRUE(grpc_cq_begin_op(cc, tag));
    grpc_cq_end_op(cc, tag, absl::OkStatus(), do_nothing_end_completion,
                   nullptr, &completion);

    ev = grpc_completion_queue_next(cc, gpr_inf_past(GPR_CLOCK_REALTIME),
                                    nullptr);
    ASSERT_EQ(ev.type, GRPC_QUEUE_TIMEOUT);

    ASSERT_EQ(grpc_completion_queue_thread_local_cache_flush(cc, &res_tag, &ok),
              1);
    ASSERT_EQ(res_tag, tag);
    ASSERT_TRUE(ok);

    ev = grpc_completion_queue_next(cc, gpr_inf_past(GPR_CLOCK_REALTIME),
                                    nullptr);
    ASSERT_EQ(ev.type, GRPC_QUEUE_TIMEOUT);

    shutdown_and_destroy(cc);
  }
}

TEST(GrpcCompletionQueueTest, TestCqTlsCacheEmpty) {
  grpc_completion_queue* cc;
  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  grpc_completion_queue_attributes attr = {};
  void* res_tag;
  int ok;

  LOG_TEST("test_cq_tls_cache_empty");

  attr.version = 1;
  attr.cq_completion_type = GRPC_CQ_NEXT;
  for (size_t i = 0; i < GPR_ARRAY_SIZE(polling_types); i++) {
    grpc_core::ExecCtx exec_ctx;  // Reset exec_ctx
    attr.cq_polling_type = polling_types[i];
    cc = grpc_completion_queue_create(
        grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);

    ASSERT_EQ(grpc_completion_queue_thread_local_cache_flush(cc, &res_tag, &ok),
              0);
    grpc_completion_queue_thread_local_cache_init(cc);
    ASSERT_EQ(grpc_completion_queue_thread_local_cache_flush(cc, &res_tag, &ok),
              0);
    shutdown_and_destroy(cc);
  }
}

TEST(GrpcCompletionQueueTest, TestShutdownThenNextPolling) {
  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  grpc_completion_queue* cc;
  grpc_completion_queue_attributes attr = {};
  grpc_event event;
  LOG_TEST("test_shutdown_then_next_polling");

  attr.version = 1;
  attr.cq_completion_type = GRPC_CQ_NEXT;
  for (size_t i = 0; i < GPR_ARRAY_SIZE(polling_types); i++) {
    attr.cq_polling_type = polling_types[i];
    cc = grpc_completion_queue_create(
        grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);
    grpc_completion_queue_shutdown(cc);
    event = grpc_completion_queue_next(cc, gpr_inf_past(GPR_CLOCK_REALTIME),
                                       nullptr);
    ASSERT_EQ(event.type, GRPC_QUEUE_SHUTDOWN);
    grpc_completion_queue_destroy(cc);
  }
}

TEST(GrpcCompletionQueueTest, TestShutdownThenNextWithTimeout) {
  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  grpc_completion_queue* cc;
  grpc_completion_queue_attributes attr = {};
  grpc_event event;
  LOG_TEST("test_shutdown_then_next_with_timeout");

  attr.version = 1;
  attr.cq_completion_type = GRPC_CQ_NEXT;
  for (size_t i = 0; i < GPR_ARRAY_SIZE(polling_types); i++) {
    attr.cq_polling_type = polling_types[i];
    cc = grpc_completion_queue_create(
        grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);

    grpc_completion_queue_shutdown(cc);
    event = grpc_completion_queue_next(cc, gpr_inf_future(GPR_CLOCK_REALTIME),
                                       nullptr);
    ASSERT_EQ(event.type, GRPC_QUEUE_SHUTDOWN);
    grpc_completion_queue_destroy(cc);
  }
}

TEST(GrpcCompletionQueueTest, TestPluck) {
  grpc_event ev;
  grpc_completion_queue* cc;
  void* tags[128];
  grpc_cq_completion completions[GPR_ARRAY_SIZE(tags)];
  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  grpc_completion_queue_attributes attr = {};
  unsigned i, j;

  LOG_TEST("test_pluck");

  for (i = 0; i < GPR_ARRAY_SIZE(tags); i++) {
    tags[i] = create_test_tag();
    for (j = 0; j < i; j++) {
      ASSERT_NE(tags[i], tags[j]);
    }
  }

  attr.version = 1;
  attr.cq_completion_type = GRPC_CQ_PLUCK;
  for (size_t pidx = 0; pidx < GPR_ARRAY_SIZE(polling_types); pidx++) {
    grpc_core::ExecCtx exec_ctx;  // reset exec_ctx
    attr.cq_polling_type = polling_types[pidx];
    cc = grpc_completion_queue_create(
        grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);

    for (i = 0; i < GPR_ARRAY_SIZE(tags); i++) {
      ASSERT_TRUE(grpc_cq_begin_op(cc, tags[i]));
      grpc_cq_end_op(cc, tags[i], absl::OkStatus(), do_nothing_end_completion,
                     nullptr, &completions[i]);
    }

    for (i = 0; i < GPR_ARRAY_SIZE(tags); i++) {
      ev = grpc_completion_queue_pluck(
          cc, tags[i], gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
      ASSERT_EQ(ev.tag, tags[i]);
    }

    for (i = 0; i < GPR_ARRAY_SIZE(tags); i++) {
      ASSERT_TRUE(grpc_cq_begin_op(cc, tags[i]));
      grpc_cq_end_op(cc, tags[i], absl::OkStatus(), do_nothing_end_completion,
                     nullptr, &completions[i]);
    }

    for (i = 0; i < GPR_ARRAY_SIZE(tags); i++) {
      ev = grpc_completion_queue_pluck(cc, tags[GPR_ARRAY_SIZE(tags) - i - 1],
                                       gpr_inf_past(GPR_CLOCK_REALTIME),
                                       nullptr);
      ASSERT_EQ(ev.tag, tags[GPR_ARRAY_SIZE(tags) - i - 1]);
    }

    shutdown_and_destroy(cc);
  }
}

TEST(GrpcCompletionQueueTest, TestPluckAfterShutdown) {
  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  grpc_event ev;
  grpc_completion_queue* cc;
  grpc_completion_queue_attributes attr = {};

  LOG_TEST("test_pluck_after_shutdown");

  attr.version = 1;
  attr.cq_completion_type = GRPC_CQ_PLUCK;
  for (size_t i = 0; i < GPR_ARRAY_SIZE(polling_types); i++) {
    attr.cq_polling_type = polling_types[i];
    cc = grpc_completion_queue_create(
        grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);
    grpc_completion_queue_shutdown(cc);
    ev = grpc_completion_queue_pluck(
        cc, nullptr, gpr_inf_future(GPR_CLOCK_REALTIME), nullptr);
    ASSERT_EQ(ev.type, GRPC_QUEUE_SHUTDOWN);
    grpc_completion_queue_destroy(cc);
  }
}

TEST(GrpcCompletionQueueTest, TestCallback) {
  grpc_completion_queue* cc;
  static void* tags[128];
  grpc_cq_completion completions[GPR_ARRAY_SIZE(tags)];
  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  grpc_completion_queue_attributes attr = {};
  unsigned i;
  static gpr_mu mu, shutdown_mu;
  static gpr_cv cv, shutdown_cv;
  static int cb_counter;
  gpr_mu_init(&mu);
  gpr_mu_init(&shutdown_mu);
  gpr_cv_init(&cv);
  gpr_cv_init(&shutdown_cv);

  LOG_TEST("test_callback");

  bool got_shutdown = false;
  class ShutdownCallback : public grpc_completion_queue_functor {
   public:
    explicit ShutdownCallback(bool* done) : done_(done) {
      functor_run = &ShutdownCallback::Run;
      inlineable = false;
    }
    ~ShutdownCallback() {}
    static void Run(grpc_completion_queue_functor* cb, int ok) {
      gpr_mu_lock(&shutdown_mu);
      *static_cast<ShutdownCallback*>(cb)->done_ = static_cast<bool>(ok);
      // Signal when the shutdown callback is completed.
      gpr_cv_signal(&shutdown_cv);
      gpr_mu_unlock(&shutdown_mu);
    }

   private:
    bool* done_;
  };
  ShutdownCallback shutdown_cb(&got_shutdown);

  attr.version = 2;
  attr.cq_completion_type = GRPC_CQ_CALLBACK;
  attr.cq_shutdown_cb = &shutdown_cb;

  for (size_t pidx = 0; pidx < GPR_ARRAY_SIZE(polling_types); pidx++) {
    int sumtags = 0;
    int counter = 0;
    cb_counter = 0;
    {
      // reset exec_ctx types
      grpc_core::ExecCtx exec_ctx;
      attr.cq_polling_type = polling_types[pidx];
      cc = grpc_completion_queue_create(
          grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);

      class TagCallback : public grpc_completion_queue_functor {
       public:
        TagCallback(int* counter, int tag) : counter_(counter), tag_(tag) {
          functor_run = &TagCallback::Run;
          // Inlineable should be false since this callback takes locks.
          inlineable = false;
        }
        ~TagCallback() {}
        static void Run(grpc_completion_queue_functor* cb, int ok) {
          ASSERT_TRUE(static_cast<bool>(ok));
          auto* callback = static_cast<TagCallback*>(cb);
          gpr_mu_lock(&mu);
          cb_counter++;
          *callback->counter_ += callback->tag_;
          if (cb_counter == GPR_ARRAY_SIZE(tags)) {
            gpr_cv_signal(&cv);
          }
          gpr_mu_unlock(&mu);
          delete callback;
        };

       private:
        int* counter_;
        int tag_;
      };

      for (i = 0; i < GPR_ARRAY_SIZE(tags); i++) {
        tags[i] = static_cast<void*>(new TagCallback(&counter, i));
        sumtags += i;
      }

      for (i = 0; i < GPR_ARRAY_SIZE(tags); i++) {
        ASSERT_TRUE(grpc_cq_begin_op(cc, tags[i]));
        grpc_cq_end_op(cc, tags[i], absl::OkStatus(), do_nothing_end_completion,
                       nullptr, &completions[i]);
      }

      gpr_mu_lock(&mu);
      while (cb_counter != GPR_ARRAY_SIZE(tags)) {
        // Wait for all the callbacks to complete.
        gpr_cv_wait(&cv, &mu, gpr_inf_future(GPR_CLOCK_REALTIME));
      }
      gpr_mu_unlock(&mu);

      shutdown_and_destroy(cc);

      gpr_mu_lock(&shutdown_mu);
      while (!got_shutdown) {
        // Wait for the shutdown callback to complete.
        gpr_cv_wait(&shutdown_cv, &shutdown_mu,
                    gpr_inf_future(GPR_CLOCK_REALTIME));
      }
      gpr_mu_unlock(&shutdown_mu);
    }

    // Run the assertions to check if the test ran successfully.
    ASSERT_EQ(sumtags, counter);
    ASSERT_TRUE(got_shutdown);
    got_shutdown = false;
  }

  gpr_cv_destroy(&cv);
  gpr_cv_destroy(&shutdown_cv);
  gpr_mu_destroy(&mu);
  gpr_mu_destroy(&shutdown_mu);
}

struct thread_state {
  grpc_completion_queue* cc;
  void* tag;
};

#ifdef GRPC_LINUX_EVENTFD
TEST(GrpcCompletionQueueTest, TestReleaseEventFdValidation) {
  ASSERT_EQ(grpc_cq_release_eventfd(nullptr), -1);

  grpc_completion_queue_attributes pluck_attr = {};
  pluck_attr.version = 1;
  pluck_attr.cq_completion_type = GRPC_CQ_PLUCK;
  pluck_attr.cq_polling_type = GRPC_CQ_DEFAULT_POLLING;
  grpc_completion_queue* pluck_cq = grpc_completion_queue_create(
      grpc_completion_queue_factory_lookup(&pluck_attr), &pluck_attr, nullptr);
  ASSERT_EQ(grpc_cq_release_eventfd(pluck_cq), -1);
  shutdown_and_destroy(pluck_cq);

  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  for (size_t i = 0; i < GPR_ARRAY_SIZE(polling_types); i++) {
    grpc_completion_queue_attributes attr = {};
    attr.version = 1;
    attr.cq_completion_type = GRPC_CQ_NEXT;
    attr.cq_polling_type = polling_types[i];
    grpc_completion_queue* cq = grpc_completion_queue_create(
        grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);
    int efd = grpc_cq_release_eventfd(cq);
    ASSERT_GE(efd, 0);
    // Releasing a second time on the same CQ must fail.
    ASSERT_EQ(grpc_cq_release_eventfd(cq), -1);

    // Initially, the eventfd must be non-blocking and have counter 0 (EAGAIN).
    eventfd_t val = 0;
    ASSERT_EQ(eventfd_read(efd, &val), -1);
    ASSERT_EQ(errno, EAGAIN);

    shutdown_and_destroy(cq);
    close(efd);
  }
}

TEST(GrpcCompletionQueueTest, TestReleaseEventFdOpAndShutdown) {
  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  for (size_t i = 0; i < GPR_ARRAY_SIZE(polling_types); i++) {
    grpc_core::ExecCtx exec_ctx;
    grpc_completion_queue_attributes attr = {};
    attr.version = 1;
    attr.cq_completion_type = GRPC_CQ_NEXT;
    attr.cq_polling_type = polling_types[i];
    grpc_completion_queue* cq = grpc_completion_queue_create(
        grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);
    int efd = grpc_cq_release_eventfd(cq);
    ASSERT_GE(efd, 0);

    int epfd = epoll_create1(EPOLL_CLOEXEC);
    ASSERT_GE(epfd, 0);
    struct epoll_event ev = {};
    ev.events = EPOLLIN;
    ev.data.fd = efd;
    ASSERT_EQ(epoll_ctl(epfd, EPOLL_CTL_ADD, efd, &ev), 0);

    struct epoll_event out_ev = {};
    ASSERT_EQ(epoll_wait(epfd, &out_ev, 1, 0), 0);

    // 1. Single op completion triggers eventfd; consumer drains until
    //    GRPC_QUEUE_TIMEOUT.
    void* tag1 = create_test_tag();
    grpc_cq_completion comp1;
    ASSERT_TRUE(grpc_cq_begin_op(cq, tag1));
    grpc_cq_end_op(cq, tag1, absl::OkStatus(), do_nothing_end_completion,
                   nullptr, &comp1);

    ASSERT_EQ(epoll_wait(epfd, &out_ev, 1, 0), 1);
    ASSERT_EQ(out_ev.data.fd, efd);
    eventfd_t val = 0;
    ASSERT_EQ(eventfd_read(efd, &val), 0);
    ASSERT_EQ(val, 1u);

    grpc_event cq_ev = grpc_completion_queue_next(
        cq, gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
    ASSERT_EQ(cq_ev.type, GRPC_OP_COMPLETE);
    ASSERT_EQ(cq_ev.tag, tag1);
    cq_ev = grpc_completion_queue_next(cq, gpr_inf_past(GPR_CLOCK_REALTIME),
                                       nullptr);
    ASSERT_EQ(cq_ev.type, GRPC_QUEUE_TIMEOUT);
    ASSERT_EQ(epoll_wait(epfd, &out_ev, 1, 0), 0);

    // 2. Multiple queued events write to eventfd only once (on empty ->
    //    non-empty transition) and must be drained in a single loop until
    //    GRPC_QUEUE_TIMEOUT without re-arming the eventfd on each pop.
    void* tags[3] = {create_test_tag(), create_test_tag(), create_test_tag()};
    grpc_cq_completion comps[3];
    for (int k = 0; k < 3; k++) {
      ASSERT_TRUE(grpc_cq_begin_op(cq, tags[k]));
      grpc_cq_end_op(cq, tags[k], absl::OkStatus(), do_nothing_end_completion,
                     nullptr, &comps[k]);
    }
    ASSERT_EQ(epoll_wait(epfd, &out_ev, 1, 0), 1);
    ASSERT_EQ(eventfd_read(efd, &val), 0);
    ASSERT_EQ(val, 1u);
    for (int k = 0; k < 3; k++) {
      cq_ev = grpc_completion_queue_next(cq, gpr_inf_past(GPR_CLOCK_REALTIME),
                                         nullptr);
      ASSERT_EQ(cq_ev.type, GRPC_OP_COMPLETE);
      ASSERT_EQ(cq_ev.tag, tags[k]);
    }
    cq_ev = grpc_completion_queue_next(cq, gpr_inf_past(GPR_CLOCK_REALTIME),
                                       nullptr);
    ASSERT_EQ(cq_ev.type, GRPC_QUEUE_TIMEOUT);
    ASSERT_EQ(epoll_wait(epfd, &out_ev, 1, 0), 0);

    // 3. Shutdown with queued events wakes epoll, and draining after
    //    eventfd_read() yields all remaining items followed by
    //    GRPC_QUEUE_SHUTDOWN.
    void* shutdown_tags[2] = {create_test_tag(), create_test_tag()};
    grpc_cq_completion shutdown_comps[2];
    for (int k = 0; k < 2; k++) {
      ASSERT_TRUE(grpc_cq_begin_op(cq, shutdown_tags[k]));
      grpc_cq_end_op(cq, shutdown_tags[k], absl::OkStatus(),
                     do_nothing_end_completion, nullptr, &shutdown_comps[k]);
    }
    grpc_completion_queue_shutdown(cq);

    ASSERT_EQ(epoll_wait(epfd, &out_ev, 1, 0), 1);
    ASSERT_EQ(eventfd_read(efd, &val), 0);
    for (int k = 0; k < 2; k++) {
      cq_ev = grpc_completion_queue_next(cq, gpr_inf_past(GPR_CLOCK_REALTIME),
                                         nullptr);
      ASSERT_EQ(cq_ev.type, GRPC_OP_COMPLETE);
      ASSERT_EQ(cq_ev.tag, shutdown_tags[k]);
    }
    cq_ev = grpc_completion_queue_next(cq, gpr_inf_past(GPR_CLOCK_REALTIME),
                                       nullptr);
    ASSERT_EQ(cq_ev.type, GRPC_QUEUE_SHUTDOWN);
    // No further wakeups after GRPC_QUEUE_SHUTDOWN has been returned.
    ASSERT_EQ(epoll_wait(epfd, &out_ev, 1, 0), 0);

    grpc_completion_queue_destroy(cq);
    close(epfd);
    close(efd);
  }
}

TEST(GrpcCompletionQueueTest, TestReleaseEventFdAfterEventsAlreadyQueued) {
  grpc_core::ExecCtx exec_ctx;
  grpc_completion_queue* cq = grpc_completion_queue_create_for_next(nullptr);
  void* tag = create_test_tag();
  grpc_cq_completion comp;
  ASSERT_TRUE(grpc_cq_begin_op(cq, tag));
  grpc_cq_end_op(cq, tag, absl::OkStatus(), do_nothing_end_completion, nullptr,
                 &comp);

  // Release eventfd AFTER an event is already sitting in the queue.
  int efd = grpc_cq_release_eventfd(cq);
  ASSERT_GE(efd, 0);
  eventfd_t val = 0;
  ASSERT_EQ(eventfd_read(efd, &val), 0);
  ASSERT_GE(val, 1u);

  grpc_event cq_ev =
      grpc_completion_queue_next(cq, gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
  ASSERT_EQ(cq_ev.type, GRPC_OP_COMPLETE);
  ASSERT_EQ(cq_ev.tag, tag);
  cq_ev =
      grpc_completion_queue_next(cq, gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
  ASSERT_EQ(cq_ev.type, GRPC_QUEUE_TIMEOUT);

  shutdown_and_destroy(cq);
  close(efd);

  // Also verify releasing eventfd AFTER shutdown has already completed on an
  // empty CQ immediately signals the eventfd for GRPC_QUEUE_SHUTDOWN.
  grpc_completion_queue* shutdown_cq =
      grpc_completion_queue_create_for_next(nullptr);
  grpc_completion_queue_shutdown(shutdown_cq);
  int shutdown_efd = grpc_cq_release_eventfd(shutdown_cq);
  ASSERT_GE(shutdown_efd, 0);
  val = 0;
  ASSERT_EQ(eventfd_read(shutdown_efd, &val), 0);
  ASSERT_GE(val, 1u);
  cq_ev = grpc_completion_queue_next(shutdown_cq,
                                     gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
  ASSERT_EQ(cq_ev.type, GRPC_QUEUE_SHUTDOWN);
  grpc_completion_queue_destroy(shutdown_cq);
  close(shutdown_efd);
}

TEST(GrpcCompletionQueueTest, TestReleaseEventFdThreadLocalCache) {
  grpc_core::ExecCtx exec_ctx;
  grpc_completion_queue* cq = grpc_completion_queue_create_for_next(nullptr);
  int efd = grpc_cq_release_eventfd(cq);
  ASSERT_GE(efd, 0);

  grpc_completion_queue_thread_local_cache_init(cq);
  void* tag1 = create_test_tag();
  void* tag2 = create_test_tag();
  grpc_cq_completion comp1;
  grpc_cq_completion comp2;
  ASSERT_TRUE(grpc_cq_begin_op(cq, tag1));
  ASSERT_TRUE(grpc_cq_begin_op(cq, tag2));
  // First op goes into TLS cache (does not signal eventfd).
  grpc_cq_end_op(cq, tag1, absl::OkStatus(), do_nothing_end_completion, nullptr,
                 &comp1);
  eventfd_t val = 0;
  ASSERT_EQ(eventfd_read(efd, &val), -1);
  ASSERT_EQ(errno, EAGAIN);

  // Second op overflows TLS cache into cqd->queue (signals eventfd).
  grpc_cq_end_op(cq, tag2, absl::OkStatus(), do_nothing_end_completion, nullptr,
                 &comp2);
  ASSERT_EQ(eventfd_read(efd, &val), 0);
  ASSERT_GE(val, 1u);

  void* flushed_tag = nullptr;
  int flushed_ok = 0;
  ASSERT_EQ(grpc_completion_queue_thread_local_cache_flush(cq, &flushed_tag,
                                                           &flushed_ok),
            1);
  ASSERT_EQ(flushed_tag, tag1);
  ASSERT_EQ(flushed_ok, 1);

  grpc_event cq_ev =
      grpc_completion_queue_next(cq, gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
  ASSERT_EQ(cq_ev.type, GRPC_OP_COMPLETE);
  ASSERT_EQ(cq_ev.tag, tag2);
  cq_ev =
      grpc_completion_queue_next(cq, gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
  ASSERT_EQ(cq_ev.type, GRPC_QUEUE_TIMEOUT);

  // Now test shutdown completing via TLS cache flush.
  grpc_completion_queue_thread_local_cache_init(cq);
  void* tag3 = create_test_tag();
  grpc_cq_completion comp3;
  ASSERT_TRUE(grpc_cq_begin_op(cq, tag3));
  grpc_cq_end_op(cq, tag3, absl::OkStatus(), do_nothing_end_completion, nullptr,
                 &comp3);
  grpc_completion_queue_shutdown(cq);
  // Shutdown is pending until the TLS cache is flushed.
  ASSERT_EQ(eventfd_read(efd, &val), -1);
  ASSERT_EQ(errno, EAGAIN);

  ASSERT_EQ(grpc_completion_queue_thread_local_cache_flush(cq, &flushed_tag,
                                                           &flushed_ok),
            1);
  ASSERT_EQ(flushed_tag, tag3);
  // Flushing the last pending event completes shutdown and signals eventfd.
  ASSERT_EQ(eventfd_read(efd, &val), 0);
  ASSERT_GE(val, 1u);
  cq_ev =
      grpc_completion_queue_next(cq, gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
  ASSERT_EQ(cq_ev.type, GRPC_QUEUE_SHUTDOWN);

  grpc_completion_queue_destroy(cq);
  close(efd);
}

TEST(GrpcCompletionQueueTest, TestReleaseEventFdConcurrentProducers) {
  constexpr int kNumProducers = 4;
  constexpr int kOpsPerProducer = 200;
  constexpr int kTotalOps = kNumProducers * kOpsPerProducer;

  grpc_completion_queue* cq = grpc_completion_queue_create_for_next(nullptr);
  int efd = grpc_cq_release_eventfd(cq);
  ASSERT_GE(efd, 0);

  int epfd = epoll_create1(EPOLL_CLOEXEC);
  ASSERT_GE(epfd, 0);
  struct epoll_event ev = {};
  ev.events = EPOLLIN;
  ev.data.fd = efd;
  ASSERT_EQ(epoll_ctl(epfd, EPOLL_CTL_ADD, efd, &ev), 0);

  std::vector<std::thread> producers;
  producers.reserve(kNumProducers);
  for (int p = 0; p < kNumProducers; p++) {
    producers.emplace_back([cq, p]() {
      for (int i = 0; i < kOpsPerProducer; i++) {
        grpc_core::ExecCtx exec_ctx;
        void* tag = reinterpret_cast<void*>(
            static_cast<intptr_t>(p * kOpsPerProducer + i + 1));
        ASSERT_TRUE(grpc_cq_begin_op(cq, tag));
        grpc_cq_end_op(
            cq, tag, absl::OkStatus(),
            [](void* /*arg*/, grpc_cq_completion* c) { delete c; }, nullptr,
            new grpc_cq_completion());
      }
    });
  }

  std::thread shutdown_thread([cq, &producers]() {
    for (auto& t : producers) {
      t.join();
    }
    grpc_completion_queue_shutdown(cq);
  });

  int completed_ops = 0;
  bool got_shutdown = false;
  while (!got_shutdown) {
    struct epoll_event out_ev = {};
    int n = epoll_wait(epfd, &out_ev, 1, 5000);
    if (n < 0 && errno == EINTR) continue;
    ASSERT_EQ(n, 1);
    eventfd_t val = 0;
    ASSERT_EQ(eventfd_read(efd, &val), 0);

    // Drain all ready completions until GRPC_QUEUE_TIMEOUT or
    // GRPC_QUEUE_SHUTDOWN.
    for (;;) {
      grpc_event cq_ev = grpc_completion_queue_next(
          cq, gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
      if (cq_ev.type == GRPC_OP_COMPLETE) {
        completed_ops++;
      } else if (cq_ev.type == GRPC_QUEUE_SHUTDOWN) {
        got_shutdown = true;
        break;
      } else {
        ASSERT_EQ(cq_ev.type, GRPC_QUEUE_TIMEOUT);
        break;
      }
    }
  }

  shutdown_thread.join();
  ASSERT_EQ(completed_ops, kTotalOps);

  grpc_completion_queue_destroy(cq);
  close(epfd);
  close(efd);
}

TEST(GrpcCompletionQueueTest,
     TestReleaseEventFdMultiConsumerAndCloseOnShutdown) {
  constexpr int kNumProducers = 4;
  constexpr int kOpsPerProducer = 200;
  constexpr int kTotalOps = kNumProducers * kOpsPerProducer;
  constexpr int kNumConsumers = 3;

  grpc_completion_queue* cq = grpc_completion_queue_create_for_next(nullptr);
  int efd = grpc_cq_release_eventfd(cq);
  ASSERT_GE(efd, 0);

  int epfd = epoll_create1(EPOLL_CLOEXEC);
  ASSERT_GE(epfd, 0);
  struct epoll_event ev = {};
  ev.events = EPOLLIN;
  ev.data.fd = efd;
  ASSERT_EQ(epoll_ctl(epfd, EPOLL_CTL_ADD, efd, &ev), 0);

  std::atomic<int> completed_ops{0};
  std::vector<std::thread> consumers;
  consumers.reserve(kNumConsumers);
  for (int c = 0; c < kNumConsumers; c++) {
    consumers.emplace_back([cq, epfd, efd, &completed_ops]() {
      while (completed_ops.load(std::memory_order_acquire) < kTotalOps) {
        struct epoll_event out_ev = {};
        int n = epoll_wait(epfd, &out_ev, 1, 5000);
        if (n < 0 && errno == EINTR) continue;
        ASSERT_EQ(n, 1);
        eventfd_t val = 0;
        if (eventfd_read(efd, &val) < 0) {
          ASSERT_EQ(errno, EAGAIN);
          continue;
        }
        for (;;) {
          grpc_event cq_ev = grpc_completion_queue_next(
              cq, gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
          if (cq_ev.type == GRPC_OP_COMPLETE) {
            if (completed_ops.fetch_add(1, std::memory_order_acq_rel) + 1 ==
                kTotalOps) {
              (void)eventfd_write(efd, 1);
            }
          } else {
            ASSERT_EQ(cq_ev.type, GRPC_QUEUE_TIMEOUT);
            break;
          }
        }
        if (completed_ops.load(std::memory_order_acquire) == kTotalOps) {
          // Cascade wakeup to any remaining sibling consumer in epoll_wait.
          (void)eventfd_write(efd, 1);
        }
      }
    });
  }

  std::vector<std::thread> producers;
  producers.reserve(kNumProducers);
  for (int p = 0; p < kNumProducers; p++) {
    producers.emplace_back([cq, p]() {
      for (int i = 0; i < kOpsPerProducer; i++) {
        grpc_core::ExecCtx exec_ctx;
        void* tag = reinterpret_cast<void*>(
            static_cast<intptr_t>(p * kOpsPerProducer + i + 1));
        ASSERT_TRUE(grpc_cq_begin_op(cq, tag));
        grpc_cq_end_op(
            cq, tag, absl::OkStatus(),
            [](void* /*arg*/, grpc_cq_completion* c) { delete c; }, nullptr,
            new grpc_cq_completion());
      }
    });
  }

  for (auto& t : producers) {
    t.join();
  }
  for (auto& t : consumers) {
    t.join();
  }
  ASSERT_EQ(completed_ops.load(), kTotalOps);

  // Clear any residual wakeup written to wake sibling consumers, then verify
  // concurrent final op completion + shutdown + immediate close(efd) upon
  // GRPC_QUEUE_SHUTDOWN never writes to a closed/reused descriptor.
  eventfd_t drain_val = 0;
  (void)eventfd_read(efd, &drain_val);

  void* final_tag = create_test_tag();
  ASSERT_TRUE(grpc_cq_begin_op(cq, final_tag));
  grpc_completion_queue_shutdown(cq);

  std::thread final_producer([cq, final_tag]() {
    grpc_core::ExecCtx exec_ctx;
    grpc_cq_end_op(
        cq, final_tag, absl::OkStatus(),
        [](void* /*arg*/, grpc_cq_completion* c) { delete c; }, nullptr,
        new grpc_cq_completion());
  });

  bool got_final_op = false;
  bool got_shutdown = false;
  while (!got_shutdown) {
    struct epoll_event out_ev = {};
    int n = epoll_wait(epfd, &out_ev, 1, 5000);
    if (n < 0 && errno == EINTR) continue;
    ASSERT_EQ(n, 1);
    eventfd_t val = 0;
    ASSERT_EQ(eventfd_read(efd, &val), 0);
    for (;;) {
      grpc_event cq_ev = grpc_completion_queue_next(
          cq, gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
      if (cq_ev.type == GRPC_OP_COMPLETE) {
        ASSERT_EQ(cq_ev.tag, final_tag);
        got_final_op = true;
      } else if (cq_ev.type == GRPC_QUEUE_SHUTDOWN) {
        got_shutdown = true;
        break;
      } else {
        ASSERT_EQ(cq_ev.type, GRPC_QUEUE_TIMEOUT);
        break;
      }
    }
  }
  ASSERT_TRUE(got_final_op);

  // Immediately close efd before joining final_producer or destroying cq, and
  // allocate a new eventfd to verify no late write hits the reused fd number.
  close(epfd);
  close(efd);
  int replacement_efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  ASSERT_GE(replacement_efd, 0);

  final_producer.join();
  ASSERT_EQ(grpc_cq_release_eventfd(cq), -1);
  grpc_completion_queue_destroy(cq);

  eventfd_t rep_val = 0;
  ASSERT_EQ(eventfd_read(replacement_efd, &rep_val), -1);
  ASSERT_EQ(errno, EAGAIN);
  close(replacement_efd);
}

TEST(GrpcCompletionQueueTest, TestReleaseEventFdWithNextBatch) {
  constexpr int kNumProducers = 4;
  constexpr int kOpsPerProducer = 200;
  constexpr int kTotalOps = kNumProducers * kOpsPerProducer;
  constexpr size_t kBatchSize = 16;

  grpc_completion_queue* cq = grpc_completion_queue_create_for_next(nullptr);
  int efd = grpc_cq_release_eventfd(cq);
  ASSERT_GE(efd, 0);

  int epfd = epoll_create1(EPOLL_CLOEXEC);
  ASSERT_GE(epfd, 0);
  struct epoll_event ev = {};
  ev.events = EPOLLIN;
  ev.data.fd = efd;
  ASSERT_EQ(epoll_ctl(epfd, EPOLL_CTL_ADD, efd, &ev), 0);

  std::vector<std::thread> producers;
  producers.reserve(kNumProducers);
  for (int p = 0; p < kNumProducers; p++) {
    producers.emplace_back([cq, p]() {
      for (int i = 0; i < kOpsPerProducer; i++) {
        grpc_core::ExecCtx exec_ctx;
        void* tag = reinterpret_cast<void*>(
            static_cast<intptr_t>(p * kOpsPerProducer + i + 1));
        ASSERT_TRUE(grpc_cq_begin_op(cq, tag));
        grpc_cq_end_op(
            cq, tag, absl::OkStatus(),
            [](void* /*arg*/, grpc_cq_completion* c) { delete c; }, nullptr,
            new grpc_cq_completion());
      }
    });
  }

  std::thread shutdown_thread([cq, &producers]() {
    for (auto& t : producers) {
      t.join();
    }
    grpc_completion_queue_shutdown(cq);
  });

  int completed_ops = 0;
  bool got_shutdown = false;
  while (!got_shutdown) {
    struct epoll_event out_ev = {};
    int n = epoll_wait(epfd, &out_ev, 1, 5000);
    if (n < 0 && errno == EINTR) continue;
    ASSERT_EQ(n, 1);
    eventfd_t val = 0;
    ASSERT_EQ(eventfd_read(efd, &val), 0);

    for (;;) {
      grpc_event batch[kBatchSize];
      size_t num_events = 0;
      grpc_completion_type type =
          grpc_cq_next_batch(cq, batch, kBatchSize, &num_events,
                             gpr_inf_past(GPR_CLOCK_REALTIME), nullptr);
      if (type == GRPC_OP_COMPLETE) {
        ASSERT_GE(num_events, 1u);
        ASSERT_LE(num_events, kBatchSize);
        for (size_t i = 0; i < num_events; i++) {
          ASSERT_EQ(batch[i].type, GRPC_OP_COMPLETE);
          ASSERT_EQ(batch[i].success, 1);
          completed_ops++;
        }
      } else if (type == GRPC_QUEUE_SHUTDOWN) {
        ASSERT_EQ(num_events, 0u);
        got_shutdown = true;
        break;
      } else {
        ASSERT_EQ(type, GRPC_QUEUE_TIMEOUT);
        ASSERT_EQ(num_events, 0u);
        break;
      }
    }
  }

  shutdown_thread.join();
  ASSERT_EQ(completed_ops, kTotalOps);

  grpc_completion_queue_destroy(cq);
  close(epfd);
  close(efd);
}
#else   // GRPC_LINUX_EVENTFD
TEST(GrpcCompletionQueueTest, TestReleaseEventFdUnsupported) {
  ASSERT_EQ(grpc_cq_release_eventfd(nullptr), -1);
  grpc_completion_queue* cq = grpc_completion_queue_create_for_next(nullptr);
  ASSERT_EQ(grpc_cq_release_eventfd(cq), -1);
  shutdown_and_destroy(cq);
}
#endif  // GRPC_LINUX_EVENTFD

TEST(GrpcCompletionQueueTest, TestNextBatchBasicAndMultiBatch) {
  grpc_cq_polling_type polling_types[] = {
      GRPC_CQ_DEFAULT_POLLING, GRPC_CQ_NON_LISTENING, GRPC_CQ_NON_POLLING};
  for (size_t pidx = 0; pidx < GPR_ARRAY_SIZE(polling_types); pidx++) {
    grpc_core::ExecCtx exec_ctx;
    grpc_completion_queue_attributes attr = {};
    attr.version = 1;
    attr.cq_completion_type = GRPC_CQ_NEXT;
    attr.cq_polling_type = polling_types[pidx];
    grpc_completion_queue* cq = grpc_completion_queue_create(
        grpc_completion_queue_factory_lookup(&attr), &attr, nullptr);

    grpc_event events[16];
    size_t num_events = 99;

    // 1. Empty queue returns GRPC_QUEUE_TIMEOUT and sets num_events to 0.
    ASSERT_EQ(grpc_cq_next_batch(cq, events, 16, &num_events,
                                 gpr_inf_past(GPR_CLOCK_REALTIME), nullptr),
              GRPC_QUEUE_TIMEOUT);
    ASSERT_EQ(num_events, 0u);

    // 2. Partial batch (5 items into capacity 16) with mixed success/error,
    //    plus verifying max_events == 0 does not consume queued items.
    void* partial_tags[5];
    grpc_cq_completion partial_comps[5];
    for (int i = 0; i < 5; i++) {
      partial_tags[i] = create_test_tag();
      ASSERT_TRUE(grpc_cq_begin_op(cq, partial_tags[i]));
      grpc_cq_end_op(
          cq, partial_tags[i],
          (i % 2 == 0) ? absl::OkStatus() : absl::InternalError("fail"),
          do_nothing_end_completion, nullptr, &partial_comps[i]);
    }

    num_events = 99;
    ASSERT_EQ(grpc_cq_next_batch(cq, nullptr, 0, &num_events,
                                 gpr_inf_future(GPR_CLOCK_REALTIME), nullptr),
              GRPC_QUEUE_TIMEOUT);
    ASSERT_EQ(num_events, 0u);

    ASSERT_EQ(grpc_cq_next_batch(cq, events, 16, &num_events,
                                 gpr_inf_past(GPR_CLOCK_REALTIME), nullptr),
              GRPC_OP_COMPLETE);
    ASSERT_EQ(num_events, 5u);
    for (int i = 0; i < 5; i++) {
      ASSERT_EQ(events[i].type, GRPC_OP_COMPLETE);
      ASSERT_EQ(events[i].tag, partial_tags[i]);
      ASSERT_EQ(events[i].success, (i % 2 == 0) ? 1 : 0);
    }
    ASSERT_EQ(grpc_cq_next_batch(cq, events, 16, &num_events,
                                 gpr_inf_past(GPR_CLOCK_REALTIME), nullptr),
              GRPC_QUEUE_TIMEOUT);
    ASSERT_EQ(num_events, 0u);

    // 3. Multi-batch drain (20 items drained with max_events = 8 -> 8, 8, 4).
    void* multi_tags[20];
    grpc_cq_completion multi_comps[20];
    for (int i = 0; i < 20; i++) {
      multi_tags[i] = create_test_tag();
      ASSERT_TRUE(grpc_cq_begin_op(cq, multi_tags[i]));
      grpc_cq_end_op(cq, multi_tags[i], absl::OkStatus(),
                     do_nothing_end_completion, nullptr, &multi_comps[i]);
    }
    const size_t expected_counts[3] = {8, 8, 4};
    size_t seen = 0;
    for (int b = 0; b < 3; b++) {
      ASSERT_EQ(grpc_cq_next_batch(cq, events, 8, &num_events,
                                   gpr_inf_past(GPR_CLOCK_REALTIME), nullptr),
                GRPC_OP_COMPLETE);
      ASSERT_EQ(num_events, expected_counts[b]);
      for (size_t i = 0; i < num_events; i++) {
        ASSERT_EQ(events[i].type, GRPC_OP_COMPLETE);
        ASSERT_EQ(events[i].tag, multi_tags[seen + i]);
        ASSERT_EQ(events[i].success, 1);
      }
      seen += num_events;
    }
    ASSERT_EQ(seen, 20u);
    ASSERT_EQ(grpc_cq_next_batch(cq, events, 8, &num_events,
                                 gpr_inf_past(GPR_CLOCK_REALTIME), nullptr),
              GRPC_QUEUE_TIMEOUT);
    ASSERT_EQ(num_events, 0u);

    // 4. Completion done() callback enqueuing a follow-up completion and
    //    querying ExecCtx::Get()->IsReadyToFinish() must not trigger
    //    ExecCtxNext::CheckReadyToFinish() to steal the newly queued completion
    //    after the batch pop loop has already completed.
    struct DoneFollowUpState {
      grpc_completion_queue* cq;
      void* follow_up_tag;
      grpc_cq_completion follow_up_comp;
      bool ready_to_finish = false;
    } follow_up_state{cq, create_test_tag(), {}, false};
    void* trigger_tag = create_test_tag();
    grpc_cq_completion trigger_comp;
    ASSERT_TRUE(grpc_cq_begin_op(cq, trigger_tag));
    ASSERT_TRUE(grpc_cq_begin_op(cq, follow_up_state.follow_up_tag));
    grpc_cq_end_op(
        cq, trigger_tag, absl::OkStatus(),
        [](void* arg, grpc_cq_completion* /*c*/) {
          auto* st = static_cast<DoneFollowUpState*>(arg);
          grpc_cq_end_op(st->cq, st->follow_up_tag, absl::OkStatus(),
                         do_nothing_end_completion, nullptr,
                         &st->follow_up_comp);
          st->ready_to_finish = grpc_core::ExecCtx::Get()->IsReadyToFinish();
        },
        &follow_up_state, &trigger_comp);

    ASSERT_EQ(grpc_cq_next_batch(cq, events, 8, &num_events,
                                 gpr_inf_past(GPR_CLOCK_REALTIME), nullptr),
              GRPC_OP_COMPLETE);
    ASSERT_EQ(num_events, 1u);
    ASSERT_EQ(events[0].tag, trigger_tag);
    ASSERT_TRUE(follow_up_state.ready_to_finish);

    ASSERT_EQ(grpc_cq_next_batch(cq, events, 8, &num_events,
                                 gpr_inf_past(GPR_CLOCK_REALTIME), nullptr),
              GRPC_OP_COMPLETE);
    ASSERT_EQ(num_events, 1u);
    ASSERT_EQ(events[0].tag, follow_up_state.follow_up_tag);

    // 5. Shutdown with remaining items drains items first, then returns
    //    GRPC_QUEUE_SHUTDOWN with num_events == 0.
    void* shutdown_tags[3];
    grpc_cq_completion shutdown_comps[3];
    for (int i = 0; i < 3; i++) {
      shutdown_tags[i] = create_test_tag();
      ASSERT_TRUE(grpc_cq_begin_op(cq, shutdown_tags[i]));
      grpc_cq_end_op(cq, shutdown_tags[i], absl::OkStatus(),
                     do_nothing_end_completion, nullptr, &shutdown_comps[i]);
    }
    grpc_completion_queue_shutdown(cq);

    ASSERT_EQ(grpc_cq_next_batch(cq, events, 16, &num_events,
                                 gpr_inf_past(GPR_CLOCK_REALTIME), nullptr),
              GRPC_OP_COMPLETE);
    ASSERT_EQ(num_events, 3u);
    for (int i = 0; i < 3; i++) {
      ASSERT_EQ(events[i].tag, shutdown_tags[i]);
    }
    num_events = 99;
    ASSERT_EQ(grpc_cq_next_batch(cq, events, 16, &num_events,
                                 gpr_inf_past(GPR_CLOCK_REALTIME), nullptr),
              GRPC_QUEUE_SHUTDOWN);
    ASSERT_EQ(num_events, 0u);

    grpc_completion_queue_destroy(cq);
  }
}

TEST(GrpcCompletionQueueTest, TestNextBatchBlockingAndLargeCapacity) {
  grpc_completion_queue* cq = grpc_completion_queue_create_for_next(nullptr);

  // 1. Test max_events > 64 (exercising heap_completions allocation path).
  constexpr size_t kLargeCount = 100;
  constexpr size_t kLargeCapacity = 128;
  std::vector<void*> large_tags(kLargeCount);
  std::vector<grpc_cq_completion> large_comps(kLargeCount);
  {
    grpc_core::ExecCtx exec_ctx;
    for (size_t i = 0; i < kLargeCount; i++) {
      large_tags[i] = create_test_tag();
      ASSERT_TRUE(grpc_cq_begin_op(cq, large_tags[i]));
      grpc_cq_end_op(cq, large_tags[i], absl::OkStatus(),
                     do_nothing_end_completion, nullptr, &large_comps[i]);
    }
  }
  std::vector<grpc_event> large_events(kLargeCapacity);
  size_t num_events = 0;
  ASSERT_EQ(
      grpc_cq_next_batch(cq, large_events.data(), kLargeCapacity, &num_events,
                         gpr_inf_past(GPR_CLOCK_REALTIME), nullptr),
      GRPC_OP_COMPLETE);
  ASSERT_EQ(num_events, kLargeCount);
  for (size_t i = 0; i < kLargeCount; i++) {
    ASSERT_EQ(large_events[i].tag, large_tags[i]);
    ASSERT_EQ(large_events[i].success, 1);
  }

  // 2. Test blocking wait when queue is initially empty and producer pushes a
  //    burst from another thread.
  constexpr int kBurstCount = 8;
  std::thread producer([cq]() {
    gpr_sleep_until(gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                                 gpr_time_from_millis(20, GPR_TIMESPAN)));
    grpc_core::ExecCtx exec_ctx;
    for (int i = 0; i < kBurstCount; i++) {
      void* tag = reinterpret_cast<void*>(static_cast<intptr_t>(i + 1));
      ASSERT_TRUE(grpc_cq_begin_op(cq, tag));
      grpc_cq_end_op(
          cq, tag, absl::OkStatus(),
          [](void* /*arg*/, grpc_cq_completion* c) { delete c; }, nullptr,
          new grpc_cq_completion());
    }
    grpc_completion_queue_shutdown(cq);
  });

  int total_drained = 0;
  for (;;) {
    grpc_event batch[16];
    size_t count = 0;
    grpc_completion_type type = grpc_cq_next_batch(
        cq, batch, 16, &count, grpc_timeout_seconds_to_deadline(5), nullptr);
    if (type == GRPC_OP_COMPLETE) {
      ASSERT_GE(count, 1u);
      total_drained += static_cast<int>(count);
    } else {
      ASSERT_EQ(type, GRPC_QUEUE_SHUTDOWN);
      ASSERT_EQ(count, 0u);
      break;
    }
  }
  producer.join();
  ASSERT_EQ(total_drained, kBurstCount);
  grpc_completion_queue_destroy(cq);

  // 3. Multi-producer + multi-consumer concurrent batch popping.
  constexpr int kNumProducers = 4;
  constexpr int kOpsPerProducer = 200;
  constexpr int kTotalOps = kNumProducers * kOpsPerProducer;
  constexpr int kNumConsumers = 4;
  grpc_completion_queue* mp_cq = grpc_completion_queue_create_for_next(nullptr);

  std::atomic<int> mp_completed{0};
  std::vector<std::thread> consumers;
  consumers.reserve(kNumConsumers);
  for (int c = 0; c < kNumConsumers; c++) {
    consumers.emplace_back([mp_cq, &mp_completed]() {
      for (;;) {
        grpc_event batch[16];
        size_t count = 0;
        grpc_completion_type type = grpc_cq_next_batch(
            mp_cq, batch, 16, &count, grpc_timeout_seconds_to_deadline(5),
            nullptr);
        if (type == GRPC_OP_COMPLETE) {
          ASSERT_GE(count, 1u);
          ASSERT_LE(count, 16u);
          mp_completed.fetch_add(static_cast<int>(count),
                                 std::memory_order_relaxed);
        } else {
          ASSERT_EQ(type, GRPC_QUEUE_SHUTDOWN);
          ASSERT_EQ(count, 0u);
          break;
        }
      }
    });
  }

  std::vector<std::thread> producers;
  producers.reserve(kNumProducers);
  for (int p = 0; p < kNumProducers; p++) {
    producers.emplace_back([mp_cq, p]() {
      for (int i = 0; i < kOpsPerProducer; i++) {
        grpc_core::ExecCtx exec_ctx;
        void* tag = reinterpret_cast<void*>(
            static_cast<intptr_t>(p * kOpsPerProducer + i + 1));
        ASSERT_TRUE(grpc_cq_begin_op(mp_cq, tag));
        grpc_cq_end_op(
            mp_cq, tag, absl::OkStatus(),
            [](void* /*arg*/, grpc_cq_completion* c) { delete c; }, nullptr,
            new grpc_cq_completion());
      }
    });
  }

  for (auto& t : producers) {
    t.join();
  }
  grpc_completion_queue_shutdown(mp_cq);
  for (auto& t : consumers) {
    t.join();
  }
  ASSERT_EQ(mp_completed.load(), kTotalOps);
  grpc_completion_queue_destroy(mp_cq);
}

int main(int argc, char** argv) {
  grpc::testing::TestEnvironment env(&argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  grpc::testing::TestGrpcScope grpc_scope;
  return RUN_ALL_TESTS();
}
