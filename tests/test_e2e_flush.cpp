// End-to-end test with flush: verify properties survive memtable -> SST
#include <iostream>
#include <filesystem>
#include <set>
#include <functional>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

using namespace cedar;

static uint16_t PropertyNameToColumnId(const std::string& name) {
  return static_cast<uint16_t>(std::hash<std::string>{}(name) & 0x0FFF);
}

// Simulate CypherEngine's NodeScan::Next with the fix
static std::map<std::string, std::string> SimulateNodeScan(
    CedarGraphStorage* storage, uint64_t entity_id,
    const std::set<std::string>& known_props) {

  std::map<std::string, std::string> result;
  result["id"] = std::to_string(entity_id);

  auto versions = storage->ScanLimit(entity_id, Timestamp(0), Timestamp::Max(), 100);

  for (const auto& [ts, desc] : versions) {
    uint16_t col_id = desc.GetColumnId();
    if (col_id == 0 || (col_id & 0xF000)) continue;  // skip label/meta columns

    // THE FIX: reverse mapping first
    std::string name = storage->GetPropertyName(col_id);
    if (name.substr(0, 4) == "col_") {
      for (const auto& prop : known_props) {
        if (PropertyNameToColumnId(prop) == col_id) {
          name = prop;
          break;
        }
      }
    }

    switch (desc.GetKind()) {
      case EntryKind::InlineInt: {
        auto v = desc.AsInlineInt();
        if (v) result[name] = std::to_string(*v);
        break;
      }
      case EntryKind::InlineFloat: {
        auto v = desc.AsInlineFloat();
        if (v) result[name] = std::to_string(*v);
        break;
      }
      case EntryKind::InlineShortStr:
        result[name] = desc.AsInlineShortStr();
        break;
      case EntryKind::ExternalRef:
        result[name] = "<blob:" + std::to_string(desc.GetPayload()) + ">";
        break;
      default:
        break;
    }
  }
  return result;
}

int main() {
  std::string data_dir = (std::filesystem::temp_directory_path() / "test_e2e_flush").string();
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);

  CedarGraphStorage* storage = nullptr;
  CedarOptions options;
  options.create_if_missing = true;
  CedarGraphStorage::Open(options, data_dir, &storage);

  // === CREATE: (n:Person {name: "Bob", age: 25, score: 95.5}) ===
  std::cout << "=== CREATE (in memtable) ===" << std::endl;

  auto write_prop = [&](const std::string& name, const std::string& val) {
    uint16_t col = PropertyNameToColumnId(name);
    storage->RegisterPropertyName(col, name);

    Descriptor desc = Descriptor::InlineInt(col, 0);  // default
    // Try int
    try {
      size_t p; int i = std::stoi(val, &p);
      if (p == val.size()) { desc = Descriptor::InlineInt(col, i); }
      else {
        // Try float
        try {
          size_t p2; float f = std::stof(val, &p2);
          if (p2 == val.size()) { desc = Descriptor::InlineFloat(col, f); }
          else {
            // String
            if (val.size() <= 4) {
              auto o = Descriptor::InlineShortStr(col, Slice(val));
              if (o) desc = *o;
            } else {
              uint32_t h = static_cast<uint32_t>(std::hash<std::string>{}(val));
              desc = Descriptor(EntryKind::ExternalRef, col, h, val.size());
            }
          }
        } catch (...) {}
      }
    } catch (...) {
      // Not int, try float
      try {
        size_t p; float f = std::stof(val, &p);
        if (p == val.size()) { desc = Descriptor::InlineFloat(col, f); }
        else {
          if (val.size() <= 4) {
            auto o = Descriptor::InlineShortStr(col, Slice(val));
            if (o) desc = *o;
          } else {
            uint32_t h = static_cast<uint32_t>(std::hash<std::string>{}(val));
            desc = Descriptor(EntryKind::ExternalRef, col, h, val.size());
          }
        }
      } catch (...) {
        // String only
        if (val.size() <= 4) {
          auto o = Descriptor::InlineShortStr(col, Slice(val));
          if (o) desc = *o;
        } else {
          uint32_t h = static_cast<uint32_t>(std::hash<std::string>{}(val));
          desc = Descriptor(EntryKind::ExternalRef, col, h, val.size());
        }
      }
    }
    storage->PutStaticVertex(100, col, desc);
  };

  write_prop("name",  "Robert");  // 6 bytes -> ExternalRef
  write_prop("age",   "25");
  write_prop("score", "95.5");

  // Verify before flush
  auto before = SimulateNodeScan(storage, 100, {"name", "age", "score"});
  std::cout << "  name="  << before["name"]
            << " age="   << before["age"]
            << " score=" << before["score"] << std::endl;

  // === FLUSH ===
  std::cout << "\n=== ForceFlush (memtable -> SST) ===" << std::endl;
  storage->ForceFlush();
  std::cout << "  Flushed." << std::endl;

  // === MATCH after flush ===
  std::cout << "\n=== MATCH (after flush, reading from SST) ===" << std::endl;
  auto after = SimulateNodeScan(storage, 100, {"name", "age", "score"});
  std::cout << "  name="  << after["name"]
            << " age="   << after["age"]
            << " score=" << after["score"] << std::endl;

  // === Verify ===
  std::cout << "\n=== Verification ===" << std::endl;
  bool ok = true;

  auto check = [&](const std::string& key, const std::string& expected) {
    auto it = after.find(key);
    if (it == after.end()) {
      std::cout << "  ❌ " << key << " MISSING" << std::endl;
      ok = false;
    } else if (it->second.find(expected) == std::string::npos) {
      std::cout << "  ❌ " << key << "=" << it->second
                << " (expected containing " << expected << ")" << std::endl;
      ok = false;
    } else {
      std::cout << "  ✅ " << key << "=" << it->second << std::endl;
    }
  };

  check("id",    "100");
  check("name",  "blob");       // ExternalRef contains "blob"
  check("age",   "25");
  check("score", "95.5");

  // No fallback col_ names
  for (const auto& [k, v] : after) {
    if (k.substr(0, 4) == "col_") {
      std::cout << "  ❌ Fallback name: " << k << std::endl;
      ok = false;
    }
  }

  std::cout << "\n" << (ok ? "🎉 ALL PASSED" : "❌ FAILED") << std::endl;

  delete storage;
  return ok ? 0 : 1;
}
