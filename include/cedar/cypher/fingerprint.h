// Copyright 2025 The Cedar Authors
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

// =============================================================================
// Cypher Query Fingerprint — Parameter-agnostic plan cache key
// =============================================================================
// Generates a stable fingerprint from a Cypher query by replacing all literal
// values with '?' placeholders and normalizing whitespace. Queries that differ
// only in their literal values (e.g. parameterized queries) will produce the
// same fingerprint, enabling plan-cache sharing.
// =============================================================================

#ifndef CEDAR_CYPHER_FINGERPRINT_H_
#define CEDAR_CYPHER_FINGERPRINT_H_

#include <string>
#include <unordered_set>

namespace cedar {
namespace cypher {

struct QueryStatement;

struct FingerprintOptions {
  // Property keys whose literal values should be preserved in the fingerprint
  // instead of being replaced with '?'. Empty set means all literals are
  // replaced (default behavior).
  std::unordered_set<std::string> preserve_property_keys;
};

// Compute a parameter-agnostic fingerprint from a raw query string.
// Replaces string literals, numeric literals, and boolean literals with '?'
// and normalizes whitespace.
std::string ComputeFingerprint(const std::string& query);

// Compute a parameter-agnostic fingerprint from a parsed AST.
// All LiteralExpr and ParameterExpr nodes are replaced with '?' tokens.
std::string ComputeFingerprint(const QueryStatement& ast);

// Compute a fingerprint from a parsed AST with options.
std::string ComputeFingerprint(const QueryStatement& ast,
                               const FingerprintOptions& options);

}  // namespace cypher
}  // namespace cedar

#endif  // CEDAR_CYPHER_FINGERPRINT_H_
