#pragma once

#include <cstdint>
#include <string_view>

namespace chainroute::sim {

using Seed = std::uint64_t;

// Deterministic, portable pseudo-random value in [0, 1) for a given
// (seed, role, a, b, c, d) combination. Same inputs always produce the
// same output on any conforming C++ compiler/standard library, because
// the implementation uses only basic integer arithmetic -- never
// std::hash<std::string> or std::uniform_real_distribution, whose
// algorithms are unspecified by the C++ standard and can differ across
// standard library implementations.
//
// This is for reproducible simulation, not cryptography: it has no
// security properties and must never be used to generate secrets.
double draw(Seed seed, std::string_view role,
            std::uint64_t a = 0, std::uint64_t b = 0,
            std::uint64_t c = 0, std::uint64_t d = 0);

}  // namespace chainroute::sim
