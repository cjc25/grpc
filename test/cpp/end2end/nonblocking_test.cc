//
//
// Copyright 2018 gRPC authors.
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

#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <deque>
#include <memory>

#include "src/core/lib/iomgr/port.h"
#include "src/core/util/grpc_check.h"
#include "src/proto/grpc/testing/echo.grpc.pb.h"
#include "test/core/test_util/port.h"
#include "test/core/test_util/test_config.h"
#include "absl/memory/memory.h"

#ifdef GRPC_POSIX_SOCKET
#include "src/core/lib/iomgr/ev_posix.h"
#endif  // GRPC_POSIX_SOCKET

#ifdef GRPC_LINUX_EVENTFD
#include <errno.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#endif  // GRPC_LINUX_EVENTFD

#include "gtest/gtest.h"

#ifdef GRPC_POSIX_SOCKET
// Thread-local variable to so that only polls from this test assert
// non-blocking (not polls from resolver, timer thread, etc), and only when the
// thread is waiting on polls caused by CompletionQueue::AsyncNext (not for
// picking a port or other reasons).
static thread_local bool g_is_nonblocking_poll;

namespace {

int maybe_assert_non_blocking_poll(struct pollfd* pfds, nfds_t nfds,
                                   int timeout) {
  // Only assert that this poll should have zero timeout if we're in the
  // middle of a zero-timeout CQ Next.
  if (g_is_nonblocking_poll) {
    GRPC_CHECK_EQ(timeout, 0);
  }
  return poll(pfds, nfds, timeout);
}

}  // namespace

namespace grpc {
namespace testing {
namespace {

void* tag(int i) { return reinterpret_cast<void*>(static_cast<intptr_t>(i)); }
int detag(void* p) { return static_cast<int>(reinterpret_cast<intptr_t>(p)); }

class NonblockingTest : public ::testing::Test {
 protected:
  NonblockingTest() {}

  void SetUp() override {
    port_ = grpc_pick_unused_port_or_die();
    server_address_ << "localhost:" << port_;

    // Setup server
    BuildAndStartServer();
  }

  bool LoopForTag(void** tag, bool* ok) {
    // Temporarily set the thread-local nonblocking poll flag so that the polls
    // caused by this loop are indeed sent by the library with zero timeout.
    bool orig_val = g_is_nonblocking_poll;
    g_is_nonblocking_poll = true;
    for (;;) {
      auto r = cq_->AsyncNext(tag, ok, gpr_time_0(GPR_CLOCK_REALTIME));
      if (r == CompletionQueue::SHUTDOWN) {
        g_is_nonblocking_poll = orig_val;
        return false;
      } else if (r == CompletionQueue::GOT_EVENT) {
        g_is_nonblocking_poll = orig_val;
        return true;
      }
    }
  }

  void TearDown() override {
    server_->Shutdown();
    void* ignored_tag;
    bool ignored_ok;
    cq_->Shutdown();
    while (LoopForTag(&ignored_tag, &ignored_ok)) {
    }
    stub_.reset();
    grpc_recycle_unused_port(port_);
  }

  void BuildAndStartServer() {
    ServerBuilder builder;
    builder.AddListeningPort(server_address_.str(),
                             grpc::InsecureServerCredentials());
    service_ = std::make_unique<grpc::testing::EchoTestService::AsyncService>();
    builder.RegisterService(service_.get());
    cq_ = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();
  }

  void ResetStub() {
    std::shared_ptr<Channel> channel = grpc::CreateChannel(
        server_address_.str(), grpc::InsecureChannelCredentials());
    stub_ = grpc::testing::EchoTestService::NewStub(channel);
  }

  void SendRpc(int num_rpcs) {
    for (int i = 0; i < num_rpcs; i++) {
      EchoRequest send_request;
      EchoRequest recv_request;
      EchoResponse send_response;
      EchoResponse recv_response;
      Status recv_status;

      ClientContext cli_ctx;
      ServerContext srv_ctx;
      grpc::ServerAsyncResponseWriter<EchoResponse> response_writer(&srv_ctx);

      send_request.set_message("hello non-blocking world");
      std::unique_ptr<ClientAsyncResponseReader<EchoResponse>> response_reader(
          stub_->PrepareAsyncEcho(&cli_ctx, send_request, cq_.get()));

      response_reader->StartCall();
      response_reader->Finish(&recv_response, &recv_status, tag(4));

      service_->RequestEcho(&srv_ctx, &recv_request, &response_writer,
                            cq_.get(), cq_.get(), tag(2));

      void* got_tag;
      bool ok;
      EXPECT_TRUE(LoopForTag(&got_tag, &ok));
      EXPECT_TRUE(ok);
      EXPECT_EQ(detag(got_tag), 2);
      EXPECT_EQ(send_request.message(), recv_request.message());

      send_response.set_message(recv_request.message());
      response_writer.Finish(send_response, Status::OK, tag(3));

      int tagsum = 0;
      int tagprod = 1;
      EXPECT_TRUE(LoopForTag(&got_tag, &ok));
      EXPECT_TRUE(ok);
      tagsum += detag(got_tag);
      tagprod *= detag(got_tag);

      EXPECT_TRUE(LoopForTag(&got_tag, &ok));
      EXPECT_TRUE(ok);
      tagsum += detag(got_tag);
      tagprod *= detag(got_tag);

      EXPECT_EQ(tagsum, 7);
      EXPECT_EQ(tagprod, 12);
      EXPECT_EQ(send_response.message(), recv_response.message());
      EXPECT_TRUE(recv_status.ok());
    }
  }

  std::unique_ptr<ServerCompletionQueue> cq_;
  std::unique_ptr<grpc::testing::EchoTestService::Stub> stub_;
  std::unique_ptr<Server> server_;
  std::unique_ptr<grpc::testing::EchoTestService::AsyncService> service_;
  std::ostringstream server_address_;
  int port_;
};

TEST_F(NonblockingTest, SimpleRpc) {
  ResetStub();
  SendRpc(10);
}

#ifdef GRPC_LINUX_EVENTFD
TEST_F(NonblockingTest, EpollEventFdRpc) {
  ResetStub();

  // Separate client CompletionQueue to test multiplexing both a client CQ
  // and a ServerCompletionQueue on a single-threaded epoll loop without
  // busy-spinning.
  CompletionQueue cli_cq;
  const int srv_efd = cq_->ReleaseEventFd();
  const int cli_efd = cli_cq.ReleaseEventFd();
  ASSERT_GE(srv_efd, 0);
  ASSERT_GE(cli_efd, 0);

  const int epfd = epoll_create1(EPOLL_CLOEXEC);
  ASSERT_GE(epfd, 0);
  auto add_to_epoll = [epfd](int fd) {
    struct epoll_event ev = {};
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    ASSERT_EQ(epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev), 0);
  };
  add_to_epoll(srv_efd);
  add_to_epoll(cli_efd);

  struct DrainedEvent {
    CompletionQueue* cq;
    CompletionQueue::NextStatus status;
    void* tag;
    bool ok;
  };
  std::deque<DrainedEvent> drained_events;

  auto wait_next_on_epoll = [epfd, srv_efd, this, &cli_cq, &drained_events](
                                CompletionQueue** out_cq, void** got_tag,
                                bool* ok) {
    while (drained_events.empty()) {
      struct epoll_event ev = {};
      int n = epoll_wait(epfd, &ev, 1, 5000);
      if (n < 0 && errno == EINTR) continue;
      GRPC_CHECK_GT(n, 0);
      eventfd_t val = 0;
      GRPC_CHECK_EQ(eventfd_read(ev.data.fd, &val), 0);
      CompletionQueue* ready_cq = (ev.data.fd == srv_efd) ? cq_.get() : &cli_cq;
      bool orig_val = g_is_nonblocking_poll;
      g_is_nonblocking_poll = true;
      // On a read from the eventfd, drain AsyncNext entirely until TIMEOUT
      // (or SHUTDOWN).
      for (;;) {
        void* drained_tag = nullptr;
        bool drained_ok = false;
        auto r = ready_cq->AsyncNext(&drained_tag, &drained_ok,
                                     gpr_time_0(GPR_CLOCK_REALTIME));
        if (r == CompletionQueue::TIMEOUT) {
          break;
        }
        drained_events.push_back({ready_cq, r, drained_tag, drained_ok});
        if (r == CompletionQueue::SHUTDOWN) {
          break;
        }
      }
      g_is_nonblocking_poll = orig_val;
    }
    DrainedEvent next = drained_events.front();
    drained_events.pop_front();
    *out_cq = next.cq;
    *got_tag = next.tag;
    *ok = next.ok;
    return next.status == CompletionQueue::GOT_EVENT;
  };

  for (int i = 0; i < 10; i++) {
    EchoRequest send_request;
    EchoRequest recv_request;
    EchoResponse send_response;
    EchoResponse recv_response;
    Status recv_status;

    ClientContext cli_ctx;
    ServerContext srv_ctx;
    grpc::ServerAsyncResponseWriter<EchoResponse> response_writer(&srv_ctx);

    send_request.set_message("hello epoll eventfd world");
    std::unique_ptr<ClientAsyncResponseReader<EchoResponse>> response_reader(
        stub_->PrepareAsyncEcho(&cli_ctx, send_request, &cli_cq));

    response_reader->StartCall();
    response_reader->Finish(&recv_response, &recv_status, tag(4));

    service_->RequestEcho(&srv_ctx, &recv_request, &response_writer, cq_.get(),
                          cq_.get(), tag(2));

    CompletionQueue* which_cq = nullptr;
    void* got_tag = nullptr;
    bool ok = false;
    EXPECT_TRUE(wait_next_on_epoll(&which_cq, &got_tag, &ok));
    EXPECT_EQ(which_cq, cq_.get());
    EXPECT_TRUE(ok);
    EXPECT_EQ(detag(got_tag), 2);
    EXPECT_EQ(send_request.message(), recv_request.message());

    send_response.set_message(recv_request.message());
    response_writer.Finish(send_response, Status::OK, tag(3));

    int tagsum = 0;
    int tagprod = 1;
    for (int k = 0; k < 2; k++) {
      EXPECT_TRUE(wait_next_on_epoll(&which_cq, &got_tag, &ok));
      EXPECT_TRUE(ok);
      tagsum += detag(got_tag);
      tagprod *= detag(got_tag);
    }
    EXPECT_EQ(tagsum, 7);
    EXPECT_EQ(tagprod, 12);
    EXPECT_EQ(send_response.message(), recv_response.message());
    EXPECT_TRUE(recv_status.ok());
  }

  cli_cq.Shutdown();
  CompletionQueue* which_cq = nullptr;
  void* got_tag = nullptr;
  bool ok = false;
  EXPECT_FALSE(wait_next_on_epoll(&which_cq, &got_tag, &ok));
  EXPECT_EQ(which_cq, &cli_cq);

  server_->Shutdown();
  cq_->Shutdown();
  while (wait_next_on_epoll(&which_cq, &got_tag, &ok)) {
    EXPECT_EQ(which_cq, cq_.get());
  }
  EXPECT_EQ(which_cq, cq_.get());

  close(epfd);
  close(cli_efd);
  close(srv_efd);
}

TEST_F(NonblockingTest, EpollEventFdRpcBatchedAsyncNext) {
  ResetStub();

  CompletionQueue cli_cq;
  const int srv_efd = cq_->ReleaseEventFd();
  const int cli_efd = cli_cq.ReleaseEventFd();
  ASSERT_GE(srv_efd, 0);
  ASSERT_GE(cli_efd, 0);

  const int epfd = epoll_create1(EPOLL_CLOEXEC);
  ASSERT_GE(epfd, 0);
  auto add_to_epoll = [epfd](int fd) {
    struct epoll_event ev = {};
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    ASSERT_EQ(epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev), 0);
  };
  add_to_epoll(srv_efd);
  add_to_epoll(cli_efd);

  struct DrainedEvent {
    CompletionQueue* cq;
    CompletionQueue::NextStatus status;
    void* tag;
    bool ok;
  };
  std::deque<DrainedEvent> drained_events;

  auto wait_next_on_epoll = [epfd, srv_efd, this, &cli_cq, &drained_events](
                                CompletionQueue** out_cq, void** got_tag,
                                bool* ok) {
    while (drained_events.empty()) {
      struct epoll_event ev = {};
      int n = epoll_wait(epfd, &ev, 1, 5000);
      if (n < 0 && errno == EINTR) continue;
      GRPC_CHECK_GT(n, 0);
      eventfd_t val = 0;
      GRPC_CHECK_EQ(eventfd_read(ev.data.fd, &val), 0);
      CompletionQueue* ready_cq = (ev.data.fd == srv_efd) ? cq_.get() : &cli_cq;
      bool orig_val = g_is_nonblocking_poll;
      g_is_nonblocking_poll = true;
      for (;;) {
        constexpr size_t kBatchSize = 16;
        void* batch_tags[kBatchSize] = {};
        bool batch_oks[kBatchSize] = {};
        size_t batch_count = 0;
        auto r = ready_cq->AsyncNext(batch_tags, batch_oks, kBatchSize,
                                     &batch_count,
                                     gpr_time_0(GPR_CLOCK_REALTIME));
        if (r == CompletionQueue::TIMEOUT) {
          GRPC_CHECK_EQ(batch_count, 0u);
          break;
        }
        if (r == CompletionQueue::SHUTDOWN) {
          GRPC_CHECK_EQ(batch_count, 0u);
          drained_events.push_back({ready_cq, r, nullptr, false});
          break;
        }
        GRPC_CHECK_EQ(r, CompletionQueue::GOT_EVENT);
        GRPC_CHECK_GE(batch_count, 1u);
        for (size_t i = 0; i < batch_count; i++) {
          drained_events.push_back(
              {ready_cq, r, batch_tags[i], batch_oks[i]});
        }
      }
      g_is_nonblocking_poll = orig_val;
    }
    DrainedEvent next = drained_events.front();
    drained_events.pop_front();
    *out_cq = next.cq;
    *got_tag = next.tag;
    *ok = next.ok;
    return next.status == CompletionQueue::GOT_EVENT;
  };

  constexpr int kBurstSize = 8;
  struct RpcState {
    EchoRequest send_request;
    EchoRequest recv_request;
    EchoResponse send_response;
    EchoResponse recv_response;
    Status recv_status;
    ClientContext cli_ctx;
    ServerContext srv_ctx;
    grpc::ServerAsyncResponseWriter<EchoResponse> response_writer{&srv_ctx};
    std::unique_ptr<ClientAsyncResponseReader<EchoResponse>> response_reader;
  };

  for (int round = 0; round < 4; round++) {
    RpcState rpcs[kBurstSize];
    for (int i = 0; i < kBurstSize; i++) {
      rpcs[i].send_request.set_message("batched epoll eventfd rpc");
      rpcs[i].response_reader = stub_->PrepareAsyncEcho(
          &rpcs[i].cli_ctx, rpcs[i].send_request, &cli_cq);
      rpcs[i].response_reader->StartCall();
      rpcs[i].response_reader->Finish(&rpcs[i].recv_response,
                                      &rpcs[i].recv_status, tag(100 + i));
      service_->RequestEcho(&rpcs[i].srv_ctx, &rpcs[i].recv_request,
                            &rpcs[i].response_writer, cq_.get(), cq_.get(),
                            tag(200 + i));
    }

    int completed_tags = 0;
    while (completed_tags < 3 * kBurstSize) {
      CompletionQueue* which_cq = nullptr;
      void* got_tag = nullptr;
      bool ok = false;
      ASSERT_TRUE(wait_next_on_epoll(&which_cq, &got_tag, &ok));
      ASSERT_TRUE(ok);
      const int t = detag(got_tag);
      completed_tags++;
      if (t >= 200 && t < 200 + kBurstSize) {
        const int idx = t - 200;
        EXPECT_EQ(which_cq, cq_.get());
        EXPECT_EQ(rpcs[idx].send_request.message(),
                  rpcs[idx].recv_request.message());
        rpcs[idx].send_response.set_message(rpcs[idx].recv_request.message());
        rpcs[idx].response_writer.Finish(rpcs[idx].send_response, Status::OK,
                                         tag(300 + idx));
      } else if (t >= 300 && t < 300 + kBurstSize) {
        EXPECT_EQ(which_cq, cq_.get());
      } else if (t >= 100 && t < 100 + kBurstSize) {
        const int idx = t - 100;
        EXPECT_EQ(which_cq, &cli_cq);
        EXPECT_EQ(rpcs[idx].send_response.message(),
                  rpcs[idx].recv_response.message());
        EXPECT_TRUE(rpcs[idx].recv_status.ok());
      } else {
        FAIL() << "Unexpected tag: " << t;
      }
    }
  }

  cli_cq.Shutdown();
  CompletionQueue* which_cq = nullptr;
  void* got_tag = nullptr;
  bool ok = false;
  EXPECT_FALSE(wait_next_on_epoll(&which_cq, &got_tag, &ok));
  EXPECT_EQ(which_cq, &cli_cq);

  server_->Shutdown();
  cq_->Shutdown();
  while (wait_next_on_epoll(&which_cq, &got_tag, &ok)) {
    EXPECT_EQ(which_cq, cq_.get());
  }
  EXPECT_EQ(which_cq, cq_.get());

  close(epfd);
  close(cli_efd);
  close(srv_efd);
}
#endif  // GRPC_LINUX_EVENTFD

}  // namespace
}  // namespace testing
}  // namespace grpc

#endif  // GRPC_POSIX_SOCKET

int main(int argc, char** argv) {
#ifdef GRPC_POSIX_SOCKET
  // Override the poll function before anything else can happen
  grpc_poll_function = maybe_assert_non_blocking_poll;

  grpc::testing::TestEnvironment env(&argc, argv);
  ::testing::InitGoogleTest(&argc, argv);

  // Start the nonblocking poll thread-local variable as false because the
  // thread that issues RPCs starts by picking a port (which has non-zero
  // timeout).
  g_is_nonblocking_poll = false;

  int ret = RUN_ALL_TESTS();

  return ret;
#else   // GRPC_POSIX_SOCKET
  (void)argc;
  (void)argv;
  return 0;
#endif  // GRPC_POSIX_SOCKET
}
