# CedarGraph 自动恢复系统部署指南

## 概述

CedarGraph 自动恢复系统提供以下功能：

- **自动故障检测**: 磁盘、内存、网络、Raft 健康状态监控
- **自动恢复动作**: 磁盘清理、服务重启、Leader 重新选举
- **健康监控**: 定期健康检查和告警
- **可视化监控**: Prometheus + Grafana 监控面板

## 快速开始

### 1. 构建项目

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core
mkdir -p build && cd build
cmake ..
cmake --build . -j8 --target storaged
```

### 2. 部署自动恢复系统

```bash
sudo ./scripts/deploy_auto_recovery.sh
```

这将：
- 安装 storaged 二进制文件
- 创建 cedar 用户和数据目录
- 安装 systemd 服务
- 配置健康监控
- 启动服务

### 3. 验证部署

```bash
# 检查服务状态
systemctl status storaged

# 运行健康检查
/usr/local/bin/cedar_health_monitor.sh

# 查看日志
journalctl -u storaged -f
```

## 配置说明

### 存储服务配置 (`/etc/cedar/storaged.conf`)

```ini
# 基本配置
node_id=1
bind_address=0.0.0.0:7000
data_dir=/var/lib/cedar/storage

# MetaD 连接
metad_endpoints=1:127.0.0.1:6000,2:127.0.0.1:6001

# 自动恢复设置
enable_auto_recovery=true
health_check_interval_sec=30
max_recovery_attempts=3

# 对等节点监控
peer_addresses=127.0.0.1:7001,127.0.0.1:7002
```

### 健康监控配置

环境变量：
- `ALERT_WEBHOOK`: Slack/Discord Webhook URL，用于告警通知

## 监控面板

### 启动监控栈

```bash
cd config
docker-compose -f docker-compose.monitoring.yml up -d
```

访问地址：
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/cedargraph)
- Alertmanager: http://localhost:9093

### 导入 Grafana Dashboard

1. 登录 Grafana (http://localhost:3000)
2. 导航到 Dashboards -> Import
3. 上传 `config/grafana/dashboards/cedar_overview.json`

## 故障恢复流程

当系统检测到以下故障时，会自动触发恢复：

| 故障类型 | 检测条件 | 恢复动作 |
|---------|---------|---------|
| 磁盘满 | 可用空间 < 10% | 清理旧日志、压缩数据 |
| 内存耗尽 | 内存使用 > 95% | 重启服务、释放缓存 |
| 网络分区 | 节点间连通性丢失 | 网络重连、分区恢复 |
| Raft 选主失败 | 长时间无 Leader | 触发重新选举 |
| 节点不可达 | 心跳超时 | 节点重启、服务恢复 |

## 手动操作

### 启用/禁用自动恢复

```bash
# 禁用自动恢复
systemctl edit storaged --force --full
# 在 [Service] 部分添加：
# Environment="CEDAR_AUTO_RECOVERY=false"

# 重新加载配置
systemctl daemon-reload
systemctl restart storaged
```

### 手动触发恢复

```cpp
// 在代码中触发特定故障类型的恢复
storage_service.TriggerRecovery(
    AutomatedRecoveryManager::FailureType::kDiskFull,
    "Manual trigger: cleanup required"
);
```

### 查看恢复历史

```bash
# 通过日志查看
grep "Recovery" /var/log/cedar/storaged.log

# 通过 API 查看 (需要实现)
curl http://localhost:7000/api/recovery/history
```

## 告警配置

### Slack 告警

1. 创建 Slack Incoming Webhook
2. 设置环境变量：`export SLACK_WEBHOOK_URL=https://hooks.slack.com/...`
3. 重启监控服务

### 邮件告警

编辑 `config/alertmanager.yml`：

```yaml
smtp_smarthost: 'smtp.gmail.com:587'
smtp_from: 'alerts@yourdomain.com'
smtp_auth_username: 'your-email@gmail.com'
smtp_auth_password: 'your-app-password'
```

## 性能调优

### 健康检查间隔

```ini
# 生产环境推荐
health_check_interval_sec=30

# 开发环境可以缩短
health_check_interval_sec=10
```

### 恢复重试次数

```ini
# 默认重试 3 次
max_recovery_attempts=3

# 关键系统可以增加重试
max_recovery_attempts=5
```

## 故障排除

### 服务无法启动

```bash
# 检查日志
journalctl -u storaged -n 100

# 检查配置
cat /etc/cedar/storaged.conf

# 检查权限
ls -la /var/lib/cedar/
ls -la /var/log/cedar/
```

### 自动恢复不工作

```bash
# 检查是否启用
grep enable_auto_recovery /etc/cedar/storaged.conf

# 检查进程
ps aux | grep storaged

# 检查健康监控日志
tail -f /var/log/cedar/health_monitor.log
```

### 监控数据不显示

```bash
# 检查 Prometheus 目标
curl http://localhost:9090/api/v1/targets

# 检查节点 exporter
curl http://localhost:9100/metrics

# 重启监控栈
docker-compose -f config/docker-compose.monitoring.yml restart
```

## 卸载

```bash
sudo ./scripts/deploy_auto_recovery.sh --uninstall
```

这将停止服务并删除所有相关文件（保留数据目录）。

## 参考

- [存储服务架构](../MULTI_RAFT_ARCHITECTURE.md)
- [生产部署指南](../DEPLOYMENT_GUIDE.md)
- [系统指标说明](../docs/METRICS.md)
