// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/cypher/value.h"
#include <cmath>
#include <functional>
#include <string>

namespace cedar {
namespace cypher {

namespace {

bool NodeEq(const Node& a, const Node& b) {
  return a.id == b.id && a.labels == b.labels && a.properties == b.properties;
}

bool RelEq(const Relationship& a, const Relationship& b) {
  return a.id == b.id && a.start_id == b.start_id && a.end_id == b.end_id && a.type == b.type;
}

bool PathEq(const Path& a, const Path& b) {
  if (a.elements.size() != b.elements.size()) return false;
  for (size_t i = 0; i < a.elements.size(); ++i) {
    const auto& va = a.elements[i];
    const auto& vb = b.elements[i];
    if (va.index() != vb.index()) return false;
    if (va.index() == 0) {
      if (!NodeEq(std::get<Node>(va), std::get<Node>(vb))) return false;
    } else {
      if (!RelEq(std::get<Relationship>(va), std::get<Relationship>(vb))) return false;
    }
  }
  return true;
}

bool NodeLt(const Node& a, const Node& b) {
  if (a.id != b.id) return a.id < b.id;
  if (a.labels != b.labels) return a.labels < b.labels;
  return a.properties < b.properties;
}

bool RelLt(const Relationship& a, const Relationship& b) {
  if (a.id != b.id) return a.id < b.id;
  if (a.start_id != b.start_id) return a.start_id < b.start_id;
  if (a.end_id != b.end_id) return a.end_id < b.end_id;
  return a.type < b.type;
}

bool PathLt(const Path& a, const Path& b) {
  size_t n = std::min(a.elements.size(), b.elements.size());
  for (size_t i = 0; i < n; ++i) {
    const auto& va = a.elements[i];
    const auto& vb = b.elements[i];
    if (va.index() != vb.index()) return va.index() < vb.index();
    if (va.index() == 0) {
      if (NodeLt(std::get<Node>(va), std::get<Node>(vb))) return true;
      if (NodeLt(std::get<Node>(vb), std::get<Node>(va))) return false;
    } else {
      if (RelLt(std::get<Relationship>(va), std::get<Relationship>(vb))) return true;
      if (RelLt(std::get<Relationship>(vb), std::get<Relationship>(va))) return false;
    }
  }
  return a.elements.size() < b.elements.size();
}

}  // namespace

bool Value::operator==(const Value& other) const {
  if (type_ != other.type_) return false;
  switch (type_) {
    case ValueType::kNull:
      return true;
    case ValueType::kBool:
      return std::get<bool>(value_) == std::get<bool>(other.value_);
    case ValueType::kInt:
      return std::get<int64_t>(value_) == std::get<int64_t>(other.value_);
    case ValueType::kFloat:
      return std::get<double>(value_) == std::get<double>(other.value_);
    case ValueType::kString:
      return std::get<std::string>(value_) == std::get<std::string>(other.value_);
    case ValueType::kTimestamp:
      return std::get<Timestamp>(value_) == std::get<Timestamp>(other.value_);
    case ValueType::kDate:
      return std::get<Date>(value_).days_since_epoch == std::get<Date>(other.value_).days_since_epoch;
    case ValueType::kTime:
      return std::get<Time>(value_).nanos_since_midnight == std::get<Time>(other.value_).nanos_since_midnight;
    case ValueType::kDateTime:
      return std::get<DateTime>(value_).timestamp == std::get<DateTime>(other.value_).timestamp;
    case ValueType::kDuration:
      return std::get<Duration>(value_).microseconds == std::get<Duration>(other.value_).microseconds;
    case ValueType::kNode: {
      const auto& a = std::get<Node>(value_);
      const auto& b = std::get<Node>(other.value_);
      return NodeEq(a, b);
    }
    case ValueType::kRelationship: {
      const auto& a = std::get<Relationship>(value_);
      const auto& b = std::get<Relationship>(other.value_);
      return RelEq(a, b);
    }
    case ValueType::kPath: {
      return PathEq(std::get<Path>(value_), std::get<Path>(other.value_));
    }
    case ValueType::kList:
      return std::get<std::vector<Value>>(value_) == std::get<std::vector<Value>>(other.value_);
    case ValueType::kMap:
      return std::get<std::map<std::string, Value>>(value_) == std::get<std::map<std::string, Value>>(other.value_);
    default:
      return false;
  }
}

bool Value::operator<(const Value& other) const {
  if (type_ != other.type_) {
    return static_cast<int>(type_) < static_cast<int>(other.type_);
  }
  switch (type_) {
    case ValueType::kNull:
      return false;
    case ValueType::kBool:
      return std::get<bool>(value_) < std::get<bool>(other.value_);
    case ValueType::kInt:
      return std::get<int64_t>(value_) < std::get<int64_t>(other.value_);
    case ValueType::kFloat:
      return std::get<double>(value_) < std::get<double>(other.value_);
    case ValueType::kString:
      return std::get<std::string>(value_) < std::get<std::string>(other.value_);
    case ValueType::kTimestamp:
      return std::get<Timestamp>(value_) < std::get<Timestamp>(other.value_);
    case ValueType::kDate:
      return std::get<Date>(value_).days_since_epoch < std::get<Date>(other.value_).days_since_epoch;
    case ValueType::kTime:
      return std::get<Time>(value_).nanos_since_midnight < std::get<Time>(other.value_).nanos_since_midnight;
    case ValueType::kDateTime:
      return std::get<DateTime>(value_).timestamp < std::get<DateTime>(other.value_).timestamp;
    case ValueType::kDuration:
      return std::get<Duration>(value_).microseconds < std::get<Duration>(other.value_).microseconds;
    case ValueType::kNode:
      return NodeLt(std::get<Node>(value_), std::get<Node>(other.value_));
    case ValueType::kRelationship:
      return RelLt(std::get<Relationship>(value_), std::get<Relationship>(other.value_));
    case ValueType::kPath:
      return PathLt(std::get<Path>(value_), std::get<Path>(other.value_));
    case ValueType::kList:
      return std::get<std::vector<Value>>(value_) < std::get<std::vector<Value>>(other.value_);
    case ValueType::kMap:
      return std::get<std::map<std::string, Value>>(value_) < std::get<std::map<std::string, Value>>(other.value_);
    default:
      return false;
  }
}

bool Value::operator>(const Value& other) const {
  return other < *this;
}

bool Value::operator<=(const Value& other) const {
  return !(other < *this);
}

bool Value::operator>=(const Value& other) const {
  return !(*this < other);
}

Value Value::operator+(const Value& other) const {
  // Null propagation
  if (IsNull() || other.IsNull()) return Value();
  
  // Numeric addition
  if (IsInt() && other.IsInt()) {
    return Value(GetInt() + other.GetInt());
  }
  if (IsNumeric() && other.IsNumeric()) {
    double a = IsInt() ? static_cast<double>(GetInt()) : GetFloat();
    double b = other.IsInt() ? static_cast<double>(other.GetInt()) : other.GetFloat();
    return Value(a + b);
  }
  
  // String concatenation
  if (IsString() || other.IsString()) {
    return Value(ToString() + other.ToString());
  }
  
  // Duration + Duration
  if (IsDuration() && other.IsDuration()) {
    return Value(GetDuration() + other.GetDuration());
  }
  
  // Timestamp + Duration
  if (IsTimestamp() && other.IsDuration()) {
    return Value(Timestamp(GetTimestamp().value() + other.GetDuration().microseconds));
  }
  if (IsDuration() && other.IsTimestamp()) {
    return Value(Timestamp(other.GetTimestamp().value() + GetDuration().microseconds));
  }
  
  return Value();
}

Value Value::operator-(const Value& other) const {
  if (IsNull() || other.IsNull()) return Value();
  
  if (IsInt() && other.IsInt()) {
    return Value(GetInt() - other.GetInt());
  }
  if (IsNumeric() && other.IsNumeric()) {
    double a = IsInt() ? static_cast<double>(GetInt()) : GetFloat();
    double b = other.IsInt() ? static_cast<double>(other.GetInt()) : other.GetFloat();
    return Value(a - b);
  }
  
  // Duration - Duration
  if (IsDuration() && other.IsDuration()) {
    return Value(GetDuration() - other.GetDuration());
  }
  
  // Timestamp - Duration
  if (IsTimestamp() && other.IsDuration()) {
    return Value(Timestamp(GetTimestamp().value() - other.GetDuration().microseconds));
  }
  
  // Timestamp - Timestamp = Duration
  if (IsTimestamp() && other.IsTimestamp()) {
    return Value(Duration(GetTimestamp().value() - other.GetTimestamp().value()));
  }
  
  return Value();
}

Value Value::operator*(const Value& other) const {
  if (IsNull() || other.IsNull()) return Value();
  
  if (IsInt() && other.IsInt()) {
    return Value(GetInt() * other.GetInt());
  }
  if (IsNumeric() && other.IsNumeric()) {
    double a = IsInt() ? static_cast<double>(GetInt()) : GetFloat();
    double b = other.IsInt() ? static_cast<double>(other.GetInt()) : other.GetFloat();
    return Value(a * b);
  }
  
  return Value();
}

Value Value::operator/(const Value& other) const {
  if (IsNull() || other.IsNull()) return Value();
  
  if (IsInt() && other.IsInt()) {
    int64_t b = other.GetInt();
    if (b == 0) return Value();
    return Value(GetInt() / b);
  }
  if (IsNumeric() && other.IsNumeric()) {
    double b = other.IsInt() ? static_cast<double>(other.GetInt()) : other.GetFloat();
    if (b == 0.0) return Value();
    double a = IsInt() ? static_cast<double>(GetInt()) : GetFloat();
    return Value(a / b);
  }
  
  return Value();
}

Value Value::operator%(const Value& other) const {
  if (IsNull() || other.IsNull()) return Value();
  
  if (IsInt() && other.IsInt()) {
    int64_t b = other.GetInt();
    if (b == 0) return Value();
    return Value(GetInt() % b);
  }
  if (IsNumeric() && other.IsNumeric()) {
    double b = other.IsInt() ? static_cast<double>(other.GetInt()) : other.GetFloat();
    if (b == 0.0) return Value();
    double a = IsInt() ? static_cast<double>(GetInt()) : GetFloat();
    return Value(std::fmod(a, b));
  }
  
  return Value();
}

Value& Value::operator+=(const Value& other) {
  *this = *this + other;
  return *this;
}

Value& Value::operator-=(const Value& other) {
  *this = *this - other;
  return *this;
}

Value Value::operator-() const {
  if (IsNull()) return Value();
  if (IsInt()) return Value(-GetInt());
  if (IsFloat()) return Value(-GetFloat());
  if (IsDuration()) return Value(Duration(-GetDuration().microseconds));
  return Value();
}

Value Value::operator+(const std::string& str) const {
  return Value(ToString() + str);
}

Value Value::operator+(const char* str) const {
  return Value(ToString() + std::string(str));
}

std::optional<Value> Value::GetProperty(const std::string& key) const {
  if (IsNode()) {
    const auto& props = GetNode().properties;
    auto it = props.find(key);
    if (it != props.end()) return it->second;
  }
  if (IsMap()) {
    const auto& map = GetMap();
    auto it = map.find(key);
    if (it != map.end()) return it->second;
  }
  return std::nullopt;
}

void Value::SetProperty(const std::string& key, const Value& val) {
  if (IsNode()) {
    GetNode().properties[key] = val;
  } else if (IsMap()) {
    GetMap()[key] = val;
  }
}

std::optional<Value> Value::GetElement(size_t index) const {
  if (IsList()) {
    const auto& list = GetList();
    if (index < list.size()) return list[index];
  }
  return std::nullopt;
}

size_t Value::Size() const {
  switch (type_) {
    case ValueType::kString:
      return GetString().size();
    case ValueType::kList:
      return GetList().size();
    case ValueType::kMap:
      return GetMap().size();
    case ValueType::kPath:
      return GetPath().Length();
    default:
      return 0;
  }
}

double Value::ToDouble() const {
  if (IsInt()) return static_cast<double>(GetInt());
  if (IsFloat()) return GetFloat();
  if (IsString()) {
    try {
      return std::stod(GetString());
    } catch (...) {
      return 0.0;
    }
  }
  return 0.0;
}

int64_t Value::ToInt() const {
  if (IsInt()) return GetInt();
  if (IsFloat()) return static_cast<int64_t>(GetFloat());
  if (IsString()) {
    try {
      return std::stoll(GetString());
    } catch (...) {
      return 0;
    }
  }
  return 0;
}

size_t Value::Hash() const {
  size_t h = static_cast<size_t>(type_);
  switch (type_) {
    case ValueType::kNull:
      return h;
    case ValueType::kBool:
      return h ^ std::hash<bool>{}(std::get<bool>(value_));
    case ValueType::kInt:
      return h ^ std::hash<int64_t>{}(std::get<int64_t>(value_));
    case ValueType::kFloat:
      return h ^ std::hash<double>{}(std::get<double>(value_));
    case ValueType::kString:
      return h ^ std::hash<std::string>{}(std::get<std::string>(value_));
    case ValueType::kTimestamp:
      return h ^ std::hash<uint64_t>{}(std::get<Timestamp>(value_).value());
    case ValueType::kList: {
      const auto& list = std::get<std::vector<Value>>(value_);
      for (const auto& v : list) {
        h = h * 31 + v.Hash();
      }
      return h;
    }
    case ValueType::kMap: {
      const auto& map = std::get<std::map<std::string, Value>>(value_);
      for (const auto& [k, v] : map) {
        h = h * 31 + std::hash<std::string>{}(k) + v.Hash();
      }
      return h;
    }
    default:
      return h;
  }
}

Value Value::AddDuration(const Duration& dur) const {
  if (IsTimestamp()) {
    return Value(Timestamp(GetTimestamp().value() + dur.microseconds));
  }
  return Value();
}

Value Value::SubDuration(const Duration& dur) const {
  if (IsTimestamp()) {
    return Value(Timestamp(GetTimestamp().value() - dur.microseconds));
  }
  return Value();
}

Duration Value::DiffTimestamp(const Value& other) const {
  if (IsTimestamp() && other.IsTimestamp()) {
    return Duration(GetTimestamp().value() - other.GetTimestamp().value());
  }
  return Duration(0);
}

bool Value::GetBool() const {
  return std::get<bool>(value_);
}

int64_t Value::GetInt() const {
  return std::get<int64_t>(value_);
}

double Value::GetFloat() const {
  return std::get<double>(value_);
}

const std::string& Value::GetString() const {
  return std::get<std::string>(value_);
}

Timestamp Value::GetTimestamp() const {
  return std::get<Timestamp>(value_);
}

Date Value::GetDate() const {
  return std::get<Date>(value_);
}

Time Value::GetTime() const {
  return std::get<Time>(value_);
}

DateTime Value::GetDateTime() const {
  return std::get<DateTime>(value_);
}

Duration Value::GetDuration() const {
  return std::get<Duration>(value_);
}

StatusOr<bool> Value::TryGetBool() const {
  if (!IsBool()) {
    return Status::InvalidArgument(
        std::string("Expected bool, got ") + ValueTypeToString(Type()));
  }
  return std::get<bool>(value_);
}

StatusOr<int64_t> Value::TryGetInt() const {
  if (!IsInt()) {
    return Status::InvalidArgument(
        std::string("Expected int, got ") + ValueTypeToString(Type()));
  }
  return std::get<int64_t>(value_);
}

StatusOr<double> Value::TryGetFloat() const {
  if (!IsFloat()) {
    return Status::InvalidArgument(
        std::string("Expected float, got ") + ValueTypeToString(Type()));
  }
  return std::get<double>(value_);
}

StatusOr<std::string> Value::TryGetString() const {
  if (!IsString()) {
    return Status::InvalidArgument(
        std::string("Expected string, got ") + ValueTypeToString(Type()));
  }
  return std::get<std::string>(value_);
}

StatusOr<Timestamp> Value::TryGetTimestamp() const {
  if (!IsTimestamp()) {
    return Status::InvalidArgument(
        std::string("Expected timestamp, got ") + ValueTypeToString(Type()));
  }
  return std::get<Timestamp>(value_);
}

StatusOr<Date> Value::TryGetDate() const {
  if (!IsDate()) {
    return Status::InvalidArgument(
        std::string("Expected date, got ") + ValueTypeToString(Type()));
  }
  return std::get<Date>(value_);
}

StatusOr<Time> Value::TryGetTime() const {
  if (!IsTime()) {
    return Status::InvalidArgument(
        std::string("Expected time, got ") + ValueTypeToString(Type()));
  }
  return std::get<Time>(value_);
}

StatusOr<DateTime> Value::TryGetDateTime() const {
  if (!IsDateTime()) {
    return Status::InvalidArgument(
        std::string("Expected datetime, got ") + ValueTypeToString(Type()));
  }
  return std::get<DateTime>(value_);
}

StatusOr<Duration> Value::TryGetDuration() const {
  if (!IsDuration()) {
    return Status::InvalidArgument(
        std::string("Expected duration, got ") + ValueTypeToString(Type()));
  }
  return std::get<Duration>(value_);
}

StatusOr<Node> Value::TryGetNode() const {
  if (!IsNode()) {
    return Status::InvalidArgument(
        std::string("Expected node, got ") + ValueTypeToString(Type()));
  }
  return std::get<Node>(value_);
}

StatusOr<Relationship> Value::TryGetRelationship() const {
  if (!IsRelationship()) {
    return Status::InvalidArgument(
        std::string("Expected relationship, got ") + ValueTypeToString(Type()));
  }
  return std::get<Relationship>(value_);
}

StatusOr<Path> Value::TryGetPath() const {
  if (!IsPath()) {
    return Status::InvalidArgument(
        std::string("Expected path, got ") + ValueTypeToString(Type()));
  }
  return std::get<Path>(value_);
}

StatusOr<std::vector<Value>> Value::TryGetList() const {
  if (!IsList()) {
    return Status::InvalidArgument(
        std::string("Expected list, got ") + ValueTypeToString(Type()));
  }
  return std::get<std::vector<Value>>(value_);
}

StatusOr<std::map<std::string, Value>> Value::TryGetMap() const {
  if (!IsMap()) {
    return Status::InvalidArgument(
        std::string("Expected map, got ") + ValueTypeToString(Type()));
  }
  return std::get<std::map<std::string, Value>>(value_);
}

Value Value::MakeNode(uint64_t id, const std::vector<std::string>& labels,
                      const std::map<std::string, Value>& props) {
  Node n;
  n.id = id;
  n.labels = labels;
  n.properties = props;
  return Value(n);
}

Value Value::MakeRelationship(uint64_t id, uint64_t start, uint64_t end,
                              const std::string& type,
                              const std::map<std::string, Value>& props) {
  Relationship r;
  r.id = id;
  r.start_id = start;
  r.end_id = end;
  r.type = type;
  r.properties = props;
  return Value(r);
}

Value Value::DateValue(int year, int month, int day) {
  return Value(Date::FromYMD(year, month, day));
}

Value Value::TimeValue(int hour, int minute, int second) {
  return Value(Time::FromHMS(hour, minute, second));
}

Value Value::DateTimeValue(int year, int month, int day,
                           int hour, int minute, int second) {
  // Approximate: convert to timestamp (simplified)
  (void)year; (void)month; (void)day; (void)hour; (void)minute; (void)second;
  return Value(DateTime(Timestamp(0), 0));
}

Value Value::DurationValue(int64_t microseconds) {
  return Value(Duration{microseconds});
}

std::string Record::ToString() const {
  return "Record";
}

Date Date::FromYMD(int year, int month, int day) {
  // Simplified stub: approximate days since epoch (not accounting for leap years correctly)
  (void)year; (void)month; (void)day;
  return Date(0);
}

Time Time::FromHMS(int hour, int minute, int second, int nanosecond) {
  int64_t nanos = ((hour * 60LL + minute) * 60LL + second) * 1000000000LL + nanosecond;
  return Time(nanos, 0);
}

std::optional<Value> ResultSet::GetFirstValue(const std::string& column) const {
  if (records.empty()) return std::nullopt;
  return records[0].Get(column);
}

const Node& Path::GetNode(size_t index) const {
  return std::get<Node>(elements[index * 2]);
}

const Relationship& Path::GetRelationship(size_t index) const {
  return std::get<Relationship>(elements[index * 2 + 1]);
}

std::string Path::ToString() const {
  return "Path";
}

}  // namespace cypher
}  // namespace cedar
