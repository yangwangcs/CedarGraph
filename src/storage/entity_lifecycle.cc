// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/entity_lifecycle.h"

namespace cedar {

std::optional<LifecycleEvent> LifecycleDescriptor::Parse(const Descriptor& desc) {
    // 检查是否为生命周期描述符（column_id = 0xFFFF）
    if (desc.GetColumnId() != kLifecycleColumnId) {
        return std::nullopt;
    }
    
    // 检查类型是否为 InlineInt
    if (desc.GetKind() != EntryKind::InlineInt) {
        return std::nullopt;
    }
    
    uint32_t payload = desc.GetPayload();
    if (payload > static_cast<uint32_t>(LifecycleEvent::Purged)) {
        return std::nullopt;
    }
    
    return static_cast<LifecycleEvent>(payload);
}

Descriptor LifecycleDescriptor::Create(LifecycleEvent event) {
    // 使用 InlineInt 类型，column_id = 0xFFFF，payload = event
    return Descriptor::InlineInt(kLifecycleColumnId, static_cast<int32_t>(event));
}

bool LifecycleDescriptor::IsLifecycleDescriptor(const Descriptor& desc) {
    return desc.GetColumnId() == kLifecycleColumnId && 
           desc.GetKind() == EntryKind::InlineInt;
}

// ========== 状态锚点实现 ==========

Descriptor LifecycleDescriptor::CreateStateAnchor(const StateAnchor& anchor) {
    // 编码：低16位存储 state (2位) + version (14位)
    // 高16位保留用于扩展
    uint32_t encoded = static_cast<uint32_t>(anchor.state) & 0x3;
    encoded |= (static_cast<uint32_t>(anchor.version) & 0x3FFF) << 2;
    return Descriptor::InlineInt(kStateAnchorColumnId, static_cast<int32_t>(encoded));
}

std::optional<StateAnchor> LifecycleDescriptor::ParseStateAnchor(const Descriptor& desc) {
    // 检查是否为状态锚点描述符
    if (desc.GetColumnId() != kStateAnchorColumnId) {
        return std::nullopt;
    }
    
    if (desc.GetKind() != EntryKind::InlineInt) {
        return std::nullopt;
    }
    
    uint32_t payload = desc.GetPayload();
    
    StateAnchor anchor;
    anchor.state = static_cast<EntityState>(payload & 0x3);
    anchor.version = static_cast<uint8_t>((payload >> 2) & 0x3FFF);
    // last_update 需要从 CedarKey 中解析
    
    return anchor;
}

bool LifecycleDescriptor::IsStateAnchorDescriptor(const Descriptor& desc) {
    return desc.GetColumnId() == kStateAnchorColumnId &&
           desc.GetKind() == EntryKind::InlineInt;
}

const char* LifecycleEventToString(LifecycleEvent event) {
    switch (event) {
        case LifecycleEvent::Unknown: return "Unknown";
        case LifecycleEvent::Created: return "Created";
        case LifecycleEvent::Deleted: return "Deleted";
        case LifecycleEvent::Recreated: return "Recreated";
        case LifecycleEvent::Purged: return "Purged";
        default: return "Invalid";
    }
}

const char* EntityStateToString(EntityState state) {
    switch (state) {
        case EntityState::NeverExisted: return "NeverExisted";
        case EntityState::Active: return "Active";
        case EntityState::Deleted: return "Deleted";
        case EntityState::Purged: return "Purged";
        default: return "Invalid";
    }
}

}  // namespace cedar
