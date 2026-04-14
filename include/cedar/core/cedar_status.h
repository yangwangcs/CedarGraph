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
// CedarStatus - 集中式严格模式错误码系统
// =============================================================================
// 专注于图拓扑一致性和时态逻辑一致性的错误处理
//
// 错误码分类：
// - 0:      成功
// - 1-9:    拓扑约束错误
// - 10-19:  时态逻辑冲突
// - 20-29:  资源与系统错误
// =============================================================================

#ifndef CEDAR_STATUS_H_
#define CEDAR_STATUS_H_

#include <cstdint>
#include <string>

namespace cedar {

// =============================================================================
// CedarCode - 集中式专用错误码枚举
// =============================================================================
enum class CedarCode : uint8_t {
    kOk = 0,

    // --- 拓扑约束错误 (Topology Constraints) ---
    kSrcNodeNotFound = 1,   // 源节点不存在（无法建立出边）
    kDstNodeNotFound = 2,   // 目标节点不存在（无法建立入边）
    kLabelMismatch = 3,     // 节点 Label 或属性定义与 Schema 不符
    kDuplicateEntity = 4,   // 幂等冲突：该实体/版本已存在

    // --- 时态逻辑冲突 (Temporal Violations) ---
    kTemporalAnachronism = 10, // 时空错位：边的发生时间早于其端点的创建时间
    kUpdateOnDeleted = 11,     // 僵尸更新：尝试更新一个在该时间点已被 DELETE 的实体
    kInvalidTimestamp = 12,    // 非法时间戳：超出系统支持范围或格式错误

    // --- 资源与系统错误 (System & Resources) ---
    kStorageFull = 20,      // 磁盘空间不足或达到 LSM-Tree 存储阈值
    kInternalError = 21,    // 内存分配失败或底层文件 IO 错误
    kInvalidArgument = 22   // 参数非法（如 ID 为 0 等）
};

// =============================================================================
// CedarStatus - 状态类
// =============================================================================
class CedarStatus {
 public:
    // 默认构造：成功状态
    CedarStatus() : code_(CedarCode::kOk) {}
    
    // 从错误码构造
    explicit CedarStatus(CedarCode code) : code_(code) {}
    
    // 从错误码和详情构造
    CedarStatus(CedarCode code, std::string details)
        : code_(code), details_(std::move(details)) {}
    
    // 静态工厂方法：成功
    static CedarStatus OK() { return CedarStatus(); }
    
    // 状态检查
    bool ok() const { return code_ == CedarCode::kOk; }
    CedarCode code() const { return code_; }
    
    // 获取错误码名称
    const char* message() const {
        switch (code_) {
            case CedarCode::kOk: return "kOk";
            case CedarCode::kSrcNodeNotFound: return "kSrcNodeNotFound";
            case CedarCode::kDstNodeNotFound: return "kDstNodeNotFound";
            case CedarCode::kLabelMismatch: return "kLabelMismatch";
            case CedarCode::kDuplicateEntity: return "kDuplicateEntity";
            case CedarCode::kTemporalAnachronism: return "kTemporalAnachronism";
            case CedarCode::kUpdateOnDeleted: return "kUpdateOnDeleted";
            case CedarCode::kInvalidTimestamp: return "kInvalidTimestamp";
            case CedarCode::kStorageFull: return "kStorageFull";
            case CedarCode::kInternalError: return "kInternalError";
            case CedarCode::kInvalidArgument: return "kInvalidArgument";
            default: return "Unknown";
        }
    }
    
    // 获取错误描述
    const char* description() const {
        switch (code_) {
            case CedarCode::kOk: return "Success";
            case CedarCode::kSrcNodeNotFound:
                return "Source node does not exist";
            case CedarCode::kDstNodeNotFound:
                return "Destination node does not exist";
            case CedarCode::kLabelMismatch:
                return "Node label or property does not match schema";
            case CedarCode::kDuplicateEntity:
                return "Entity/version already exists";
            case CedarCode::kTemporalAnachronism:
                return "Event time precedes entity creation time";
            case CedarCode::kUpdateOnDeleted:
                return "Attempting to update a deleted entity";
            case CedarCode::kInvalidTimestamp:
                return "Invalid timestamp format or out of range";
            case CedarCode::kStorageFull:
                return "Storage capacity exceeded";
            case CedarCode::kInternalError:
                return "Internal system error";
            case CedarCode::kInvalidArgument:
                return "Invalid argument";
            default: return "Unknown error";
        }
    }
    
    // 生成完整错误信息
    std::string ToString() const {
        std::string result = "[" + std::string(message()) + "] ";
        if (details_.empty()) {
            result += description();
        } else {
            result += details_;
        }
        return result;
    }
    
    // 便捷检查：是否为拓扑错误
    bool IsTopologyError() const {
        return code_ == CedarCode::kSrcNodeNotFound ||
               code_ == CedarCode::kDstNodeNotFound ||
               code_ == CedarCode::kLabelMismatch ||
               code_ == CedarCode::kDuplicateEntity;
    }
    
    // 便捷检查：是否为时态错误
    bool IsTemporalError() const {
        return code_ == CedarCode::kTemporalAnachronism ||
               code_ == CedarCode::kUpdateOnDeleted ||
               code_ == CedarCode::kInvalidTimestamp;
    }
    
    // 添加上下文信息（链式调用）
    CedarStatus& WithEntity(uint64_t entity_id) {
        details_ += " [Entity: " + std::to_string(entity_id) + "]";
        return *this;
    }
    
    CedarStatus& WithTimestamp(uint64_t ts) {
        details_ += " [Timestamp: " + std::to_string(ts) + "]";
        return *this;
    }

 private:
    CedarCode code_;
    std::string details_;
};

// =============================================================================
// 便捷宏
// =============================================================================

// 如果状态错误则返回
#define CEDAR_RETURN_IF_ERROR(status) \
    do { \
        auto _s = (status); \
        if (!_s.ok()) return _s; \
    } while(0)

// 如果状态错误则执行清理并返回
#define CEDAR_RETURN_IF_ERROR_CLEANUP(status, cleanup) \
    do { \
        auto _s = (status); \
        if (!_s.ok()) { \
            { cleanup; } \
            return _s; \
        } \
    } while(0)

}  // namespace cedar

#endif  // CEDAR_STATUS_H_
