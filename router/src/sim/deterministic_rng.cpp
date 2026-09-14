#include "chainroute/sim/deterministic_rng.hpp"

namespace chainroute::sim {

namespace {

constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

std::uint64_t fnv1aByte(std::uint64_t state, std::uint8_t byte) {
    state ^= byte;
    state *= kFnvPrime;
    return state;
}

// splitmix64 finalizer: cheap, well-distributed avalanche mixing.
std::uint64_t splitmix64(std::uint64_t state) {
    state += 0x9E3779B97F4A7C15ULL;
    std::uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

}  // namespace

double draw(Seed seed, std::string_view role,
            std::uint64_t a, std::uint64_t b, std::uint64_t c, std::uint64_t d) {
    std::uint64_t state = kFnvOffsetBasis;
    for (unsigned char byte : role) {
        state = fnv1aByte(state, byte);
    }
    for (int i = 0; i < 8; ++i) {
        state = fnv1aByte(state, static_cast<std::uint8_t>(seed >> (8 * i)));
    }

    state = splitmix64(state ^ a);
    state = splitmix64(state ^ b);
    state = splitmix64(state ^ c);
    state = splitmix64(state ^ d);

    // Top 53 bits give a double in [0, 1) with full mantissa precision.
    constexpr int kMantissaBits = 53;
    constexpr double kScale = 1.0 / static_cast<double>(1ULL << kMantissaBits);
    return static_cast<double>(state >> (64 - kMantissaBits)) * kScale;
}

}  // namespace chainroute::sim
