# P4 Fix: JWT Constant-Time Signature Comparison

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** Replace non-constant-time JWT signature comparison with a constant-time comparison to prevent timing attacks.

**Architecture:** Implement a `ConstantTimeCompare` helper using OpenSSL's `CRYPTO_memcmp` or a byte-by-byte XOR-based comparison, then use it in `ParseJWT`.

**Tech Stack:** C++17, OpenSSL, gtest

---

## File Map

| File | Responsibility |
|------|----------------|
| `src/dtx/security/security_manager.cc` | Add ConstantTimeCompare helper, use in ParseJWT |
| `tests/dtx/test_security_manager.cc` (or new file) | Add timing-attack resistance test |

---

### Task 1: Add ConstantTimeCompare Helper

**Files:**
- Modify: `src/dtx/security/security_manager.cc`

- [ ] **Step 1: Add helper function before ParseJWT**

Find `ParseJWT` (around line 500) and add before it:

```cpp
// Constant-time string comparison to prevent timing attacks.
// Returns true iff a == b. Execution time depends only on |a| and |b|,
// not on the content of the strings.
bool ConstantTimeCompare(const std::string& a, const std::string& b) {
  if (a.size() != b.size()) return false;
  volatile unsigned char result = 0;
  for (size_t i = 0; i < a.size(); ++i) {
    result |= static_cast<unsigned char>(a[i] ^ b[i]);
  }
  return result == 0;
}
```

- [ ] **Step 2: Replace != with ConstantTimeCompare in ParseJWT**

At line 583, replace:
```cpp
  if (encoded_signature != expected_encoded_sig) {
    return Status::InvalidArgument("Invalid JWT signature");
  }
```

With:
```cpp
  if (!ConstantTimeCompare(encoded_signature, expected_encoded_sig)) {
    return Status::InvalidArgument("Invalid JWT signature");
  }
```

- [ ] **Step 3: Build cedar target**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target cedar -j$(sysctl -n hw.ncpu)
```

- [ ] **Step 4: Commit**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core && git add src/dtx/security/security_manager.cc && git commit -m "fix(security): use constant-time comparison for JWT signature verification"
```

---

### Task 2: Add Unit Test for ConstantTimeCompare

**Files:**
- Create: `tests/dtx/test_security_manager_jwt.cc`

- [ ] **Step 1: Write test file**

```cpp
#include <gtest/gtest.h>
#include <string>
#include <vector>

// Re-declare the helper (or include the header if exported)
namespace cedar {
namespace dtx {
namespace security {
extern bool ConstantTimeCompare(const std::string& a, const std::string& b);
}
}
}

using namespace cedar::dtx::security;

TEST(ConstantTimeCompareTest, EqualStringsReturnTrue) {
  EXPECT_TRUE(ConstantTimeCompare("abc", "abc"));
  EXPECT_TRUE(ConstantTimeCompare(std::string(256, 'x'), std::string(256, 'x')));
}

TEST(ConstantTimeCompareTest, DifferentStringsReturnFalse) {
  EXPECT_FALSE(ConstantTimeCompare("abc", "abd"));
  EXPECT_FALSE(ConstantTimeCompare("abc", "ab"));
  EXPECT_FALSE(ConstantTimeCompare("ab", "abc"));
  EXPECT_FALSE(ConstantTimeCompare("", "a"));
}

TEST(ConstantTimeCompareTest, DifferentLengthReturnsFalse) {
  EXPECT_FALSE(ConstantTimeCompare("short", "longer_string"));
}
```

- [ ] **Step 2: Register test in tests/CMakeLists.txt**

Add:
```cmake
add_executable(test_security_manager_jwt dtx/test_security_manager_jwt.cc)
target_link_libraries(test_security_manager_jwt ${CEDAR_TEST_LIBS})
gtest_discover_tests(test_security_manager_jwt)
```

- [ ] **Step 3: Build and run test**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake . >/dev/null && cmake --build . --target test_security_manager_jwt -j$(sysctl -n hw.ncpu) && ./tests/test_security_manager_jwt
```
Expected: 3 tests pass.

- [ ] **Step 4: Commit**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core && git add tests/dtx/test_security_manager_jwt.cc tests/CMakeLists.txt && git commit -m "test(security): add constant-time comparison unit tests"
```

---

### Task 3: Full Regression Test

- [ ] **Step 1: Run full test suite**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && ctest --output-on-failure -j$(sysctl -n hw.ncpu)
```
Expected: 1285/1285 passed, 0 failed.
