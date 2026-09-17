#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <sstream>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "chainroute/sim/network_simulator.hpp"
#include "chainroute_service/routing_service.hpp"
#include "metrics.hpp"

namespace {

std::unique_ptr<grpc::Server> g_server;

// RunMetricsListener serves a Prometheus /metrics endpoint over a raw TCP
// socket -- deliberately not a general-purpose HTTP server. This endpoint
// serves exactly one fixed body regardless of what's requested, so there is
// no request parsing beyond discarding the incoming bytes (Phase 10 design
// doc §4: avoid pulling in an HTTP library for a trivial fixed response).
//
// Shutdown: this loop polls the listening socket with a 1-second timeout
// before calling accept(), so it wakes up periodically to check shouldStop
// even when no client is connecting, letting the thread exit cleanly (and
// be joined) instead of blocking forever.
//
// NOTE: an earlier version of this function relied on SO_RCVTIMEO to time
// out accept() directly (as a naive reading of the BSD socket docs
// suggests). That does NOT work: SO_RCVTIMEO governs recv()-family calls,
// and POSIX does not guarantee it bounds accept()'s wait -- observed
// directly here as accept() blocking forever on macOS with SO_RCVTIMEO set,
// which left the listener thread unjoinable and hung the whole process at
// shutdown. poll()+accept() is the portable way to get a timed wait on a
// listening socket.
void RunMetricsListener(const chainroute::RouteMetrics& metrics, int port, std::atomic<bool>& shouldStop) {
    int serverFd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd < 0) {
        return;
    }
    int opt = 1;
    setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (bind(serverFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(serverFd);
        return;
    }
    listen(serverFd, 16);

    while (!shouldStop.load()) {
        pollfd pfd{};
        pfd.fd = serverFd;
        pfd.events = POLLIN;
        const int pollResult = poll(&pfd, 1, /*timeout_ms=*/1000);
        if (pollResult <= 0) {
            continue;  // poll timeout or transient error -- loop and re-check shouldStop
        }

        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);
        int clientFd = accept(serverFd, reinterpret_cast<sockaddr*>(&clientAddr), &clientLen);
        if (clientFd < 0) {
            continue;  // transient accept error (e.g. connection reset before accept) -- loop and re-check shouldStop
        }
        // Bound recv()/send() on this client socket: a connected-but-idle (or
        // malicious) client that never sends anything would otherwise wedge
        // this single-threaded loop in recv() forever, which also blocks
        // metricsThread.join() at shutdown and makes SIGTERM ineffective
        // (only SIGKILL would recover). This timeout is per-client-socket,
        // not the listening socket, so it doesn't affect the poll()+accept()
        // wait above.
        struct timeval tv{2, 0};  // 2 second timeout
        setsockopt(clientFd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(clientFd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        char buf[512];
        recv(clientFd, buf, sizeof(buf), 0);  // discard the request line -- this endpoint serves exactly one fixed body

        std::string body = metrics.PrometheusText();
        std::ostringstream response;
        response << "HTTP/1.1 200 OK\r\n"
                  << "Content-Type: text/plain; version=0.0.4\r\n"
                  << "Content-Length: " << body.size() << "\r\n"
                  << "Connection: close\r\n\r\n"
                  << body;
        std::string responseStr = response.str();
        send(clientFd, responseStr.data(), responseStr.size(), 0);
        close(clientFd);
    }
    close(serverFd);
}

// NOTE: grpc::Server::Shutdown() must not be called from an async-signal
// handler. It acquires internal (Abseil) mutexes, and if the interrupted
// thread already held one of those same mutexes, calling Shutdown()
// straight from a signal handler (as gRPC's own examples sometimes show)
// deadlocks/aborts with "illegal recursion into Mutex code" -- observed
// directly when testing this server with std::signal + a handler that
// called Shutdown() inline. Instead, block SIGINT/SIGTERM on all threads
// and let a dedicated thread synchronously consume them via sigwait(),
// which runs in normal (non-signal) thread context where calling into
// gRPC is safe.
void waitForShutdownSignal(sigset_t signalSet) {
    int receivedSignal = 0;
    sigwait(&signalSet, &receivedSignal);
    if (g_server) {
        g_server->Shutdown();
    }
}

}  // namespace

int main(int argc, char** argv) {
    // Block SIGINT/SIGTERM on the main thread FIRST, before constructing
    // anything that might spawn worker threads (notably
    // grpc::ServerBuilder::BuildAndStart(), which starts gRPC's own
    // completion-queue threads). POSIX signal masks are per-thread and are
    // inherited by a new thread only from its creator's mask at the moment
    // of creation. If this block happened after BuildAndStart(), gRPC's
    // internal threads would already have these signals unblocked, and the
    // kernel could deliver SIGTERM to one of them instead of to our
    // dedicated signal-handling thread below -- which was observed to
    // terminate the process immediately (default disposition) without ever
    // running our graceful-shutdown path.
    sigset_t signalSet;
    sigemptyset(&signalSet);
    sigaddset(&signalSet, SIGINT);
    sigaddset(&signalSet, SIGTERM);
    // A non-interactive shell auto-SIG_IGNs SIGINT/SIGQUIT for `&`-backgrounded
    // children; that disposition is inherited by this process and is NOT
    // affected by pthread_sigmask() below (masking only controls blocking,
    // not disposition), so an inherited SIG_IGN would cause the kernel to
    // discard SIGINT before sigwait() ever sees it. Reset both to default
    // first so blocking + sigwait() can reliably observe them.
    std::signal(SIGINT, SIG_DFL);
    std::signal(SIGTERM, SIG_DFL);
    pthread_sigmask(SIG_BLOCK, &signalSet, nullptr);

    std::uint64_t seed = 42;
    std::string listenAddress = "0.0.0.0:50051";

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg.rfind("--seed=", 0) == 0) {
            seed = std::strtoull(arg.c_str() + 7, nullptr, 10);
        } else if (arg.rfind("--listen-address=", 0) == 0) {
            listenAddress = arg.substr(std::strlen("--listen-address="));
        }
    }

    chainroute::sim::NetworkSimulator simulator(seed);
    chainroute::RouteMetrics metrics;
    chainroute_service::RoutingServiceImpl service(simulator, metrics);

    int metricsPort = 9102;
    if (const char* envPort = std::getenv("METRICS_PORT")) {
        metricsPort = std::atoi(envPort);
    }
    std::atomic<bool> stopMetricsListener{false};
    std::thread metricsThread(RunMetricsListener, std::cref(metrics), metricsPort, std::ref(stopMetricsListener));

    grpc::EnableDefaultHealthCheckService(true);

    grpc::ServerBuilder builder;
    // Disable SO_REUSEPORT (enabled by gRPC C++ by default): with it on, a
    // second process binding the same already-bound address "succeeds"
    // silently and the kernel load-balances traffic between both processes,
    // so a genuine bind conflict must be surfaced via the selected-port
    // out-parameter below instead.
    builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
    int selectedPort = 0;
    builder.AddListeningPort(listenAddress, grpc::InsecureServerCredentials(), &selectedPort);
    builder.RegisterService(&service);

    g_server = builder.BuildAndStart();
    if (!g_server || selectedPort == 0) {
        std::cerr << "Failed to start server on " << listenAddress << "\n";
        stopMetricsListener.store(true);
        metricsThread.join();
        return 1;
    }

    std::thread signalThread(waitForShutdownSignal, signalSet);

    std::cout << "chainroute_service_server listening on " << listenAddress
              << " (seed=" << seed << ")\n"
              << std::flush;
    g_server->Wait();
    signalThread.join();

    stopMetricsListener.store(true);
    metricsThread.join();

    std::cout << "chainroute_service_server shut down\n" << std::flush;
    return 0;
}
