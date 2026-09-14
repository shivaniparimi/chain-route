#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "chainroute/sim/network_simulator.hpp"
#include "chainroute_service/routing_service.hpp"

namespace {

std::unique_ptr<grpc::Server> g_server;

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
    chainroute_service::RoutingServiceImpl service(simulator);

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
        return 1;
    }

    std::thread signalThread(waitForShutdownSignal, signalSet);

    std::cout << "chainroute_service_server listening on " << listenAddress
              << " (seed=" << seed << ")\n"
              << std::flush;
    g_server->Wait();
    signalThread.join();
    std::cout << "chainroute_service_server shut down\n" << std::flush;
    return 0;
}
