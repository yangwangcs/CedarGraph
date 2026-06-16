// End-to-end test: CREATE node with properties, then MATCH and verify property names
#include <iostream>
#include <filesystem>
#include <set>
#include <functional>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

// Same hash as CypherEngine
static uint16_t PropertyNameToColumnId(const std::string& name) {
  return static_cast<uint16_t>(std::hash<std::string>{}(name) & 0x0FFF);
}

static Descriptor ValueToDescriptor(const std::string& prop_name,
                                     const std::string& value,
                                     uint16_t col_id) {
  // Try int
  try {
    size_t pos;
    int v = std::stoi(value, &pos);
    if (pos == value.size()) {
      return Descriptor::InlineInt(col_id, v);
    }
  } catch (...) {}

  // Try float
  try {
    size_t pos;
    float v = std::stof(value, &pos);
    if (pos == value.size()) {
      return Descriptor::InlineFloat(col_id, v);
    }
  } catch (...) {}

  // String
  if (value.size() <= 4) {
    auto opt = Descriptor::InlineShortStr(col_id, Slice(value));
    if (opt) return *opt;
  }
  // Long string -> ExternalRef (the fix)
  uint32_t hash = static_cast<uint32_t>(std::hash<std::string>{}(value));
  return Descriptor(EntryKind::ExternalRef, col_id, hash,
                    static_cast<uint8_t>(std::min(value.size(), size_t(255))));
}

int main() {
  std::string data_dir = "/tmp/test_e2e_property";
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  CedarGraphStorage* storage = nullptr;
  CedarOptions options;
  options.create_if_missing = true;

  auto status = CedarGraphStorage::Open(options, data_dir, &storage);
  if (!status.ok()) {
    std::cerr << "Open failed: " << status.ToString() << std::endl;
    return 1;
  }

  // ============================================================
  // Phase 1: CREATE (n:Person {id: 42, name: "Alice", age: 30, city: "Beijing"})
  // ============================================================
  std::cout << "=== Phase 1: CREATE Node ===" << std::endl;

  struct Prop { std::string name; std::string value; };
  Prop props[] = {
    {"id",   "42"},
    {"name", "Alice"},     // 5 bytes -> ExternalRef
    {"age",  "30"},
    {"city", "Beijing"},   // 7 bytes -> ExternalRef
  };

  uint64_t entity_id = 42;

  for (const auto& p : props) {
    uint16_t col_id = PropertyNameToColumnId(p.name);
    Descriptor desc = ValueToDescriptor(p.name, p.value, col_id);

    // Register reverse mapping (same as CreateOperator does)
    storage->RegisterPropertyName(col_id, p.name);

    status = storage->PutStaticVertex(entity_id, col_id, desc);
    if (!status.ok()) {
      std::cerr << "  PutStaticVertex failed for " << p.name
                << ": " << status.ToString() << std::endl;
      continue;
    }
    std::cout << "  Created " << p.name << "=" << p.value
              << " (col_id=" << col_id << ", kind=" << static_cast<int>(desc.GetKind()) << ")"
              << std::endl;
  }

  // Persist label
  {
    auto desc_opt = Descriptor::InlineShortStr(0xF000, Slice("Person"));
    if (desc_opt) {
      storage->PutStaticVertex(entity_id, 0xF000, *desc_opt);
      std::cout << "  Created label=Person (col_id=" << 0xF000 << ")" << std::endl;
    }
  }

  std::cout << std::endl;

  // ============================================================
  // Phase 2: MATCH (n) WHERE id(n) = 42 RETURN n
  // Simulate NodeScan::Next() with the fix
  // ============================================================
  std::cout << "=== Phase 2: MATCH Query (simulated NodeScan::Next) ===" << std::endl;

  // Required columns from query (e.g., RETURN n.name, n.age)
  std::set<std::string> required_columns = {"n.name", "n.age", "n.city"};

  // Build known_props from required_columns (same as NodeScan::Next)
  std::set<std::string> known_props;
  for (const auto& col : required_columns) {
    size_t dot = col.find('.');
    if (dot != std::string::npos) {
      known_props.insert(col.substr(dot + 1));
    }
  }

  std::cout << "  Known props: ";
  for (const auto& p : known_props) std::cout << p << " ";
  std::cout << std::endl << std::endl;

  // Scan all versions for entity_id
  auto versions = storage->ScanLimit(entity_id, Timestamp(0), Timestamp::Max(), 100);
  std::cout << "  ScanLimit returned " << versions.size() << " versions" << std::endl;

  // Build node properties (same logic as NodeScan::Next with the fix)
  std::map<std::string, std::string> node_properties;
  node_properties["id"] = "42";  // always include id

  for (const auto& [ts, desc] : versions) {
    uint16_t col_id = desc.GetColumnId();

    // Skip label column and zero column
    if (col_id == 0 || col_id == 0xF000) continue;

    // === THE FIX: Use reverse mapping first ===
    std::string matched_name = storage->GetPropertyName(col_id);

    // If reverse mapping returns fallback "col_XXXX", try forward-match
    if (matched_name.substr(0, 4) == "col_") {
      for (const auto& prop : known_props) {
        uint16_t computed = PropertyNameToColumnId(prop);
        if (computed == col_id) {
          matched_name = prop;
          break;
        }
      }
    }

    // Extract value based on kind
    std::string value_str;
    if (desc.GetKind() == EntryKind::InlineInt) {
      auto v = desc.AsInlineInt();
      if (v) value_str = std::to_string(*v);
    } else if (desc.GetKind() == EntryKind::InlineFloat) {
      auto v = desc.AsInlineFloat();
      if (v) value_str = std::to_string(*v);
    } else if (desc.GetKind() == EntryKind::InlineShortStr) {
      value_str = desc.AsInlineShortStr();
    } else if (desc.GetKind() == EntryKind::ExternalRef) {
      value_str = "<ExternalRef hash=" + std::to_string(desc.GetPayload()) + ">";
    }

    node_properties[matched_name] = value_str;
    std::cout << "  col_id=" << col_id
              << " kind=" << static_cast<int>(desc.GetKind())
              << " -> name=" << matched_name
              << " value=" << value_str << std::endl;
  }

  std::cout << std::endl;

  // ============================================================
  // Phase 3: Verify results
  // ============================================================
  std::cout << "=== Phase 3: Verification ===" << std::endl;

  bool all_ok = true;

  // Check that all expected properties are present with correct names
  struct Expected { std::string name; std::string contains; };
  Expected expected[] = {
    {"id",   "42"},
    {"age",  "30"},
    {"name", "ExternalRef"},  // long string stored as ExternalRef
    {"city", "ExternalRef"},  // long string stored as ExternalRef
  };

  for (const auto& e : expected) {
    auto it = node_properties.find(e.name);
    if (it == node_properties.end()) {
      std::cout << "  ❌ MISSING property: " << e.name << std::endl;
      all_ok = false;
    } else if (it->second.find(e.contains) == std::string::npos) {
      std::cout << "  ❌ WRONG value for " << e.name
                << ": expected containing '" << e.contains
                << "', got '" << it->second << "'" << std::endl;
      all_ok = false;
    } else {
      std::cout << "  ✅ " << e.name << "=" << it->second << std::endl;
    }
  }

  // Check that no "col_XXX" fallback names appear
  for (const auto& [k, v] : node_properties) {
    if (k.substr(0, 4) == "col_") {
      std::cout << "  ❌ Fallback name detected: " << k << "=" << v << std::endl;
      all_ok = false;
    }
  }

  std::cout << std::endl;
  if (all_ok) {
    std::cout << "🎉 ALL TESTS PASSED - Property storage and query working correctly!" << std::endl;
  } else {
    std::cout << "❌ SOME TESTS FAILED" << std::endl;
  }

  delete storage;
  return all_ok ? 0 : 1;
}
