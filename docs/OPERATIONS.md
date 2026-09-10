# 运行与告警

## 健康检查

至少监控以下状态：

- 主服务和sidecar是否active、PID是否意外变化；
- health文件更新时间；
- 各链connected和当前endpoint slot；
- checkpoint或日志计数是否前进；
- 补全任务pending/done分布；
- outbox pending/sending/sent/unknown分布；
- API 429、5xx、网络错误和最近成功时间；
- 进程RSS、cgroup memory events和磁盘增长。

## 告警去抖

数据源必须连续异常达到阈值后才通知。恢复也应持续稳定一段时间后再发送一次恢复消息。主节点切到备用但仍正常采集时，告警应说明“备用仍在工作”，不能描述为整体停机。

健康告警不得停止候选发送器。候选outbox与健康通知使用独立状态。

## 常见事件

### RPC 429

确认是账户额度、每秒限制还是单方法成本；停止无意义的主节点探测，降低重复HTTP调用，必要时切换不同故障域。不要通过更高频重试解决429。

### WSS断开

检查最后日志时间、checkpoint和辅助HTTP错误。区块时间、回填或数据库异常不应伪装成WSS断开；辅助字段允许安全降级时，应保持实时流继续。

### collector ready=false

检查采集器health新鲜度、coverage start和watermark。进程active不代表事件循环没有被内存或SQLite扫描阻塞。

### 没有信号

依次检查：发现数量、规模拦截、分阶段重试、DEX市场、安全证据、开发者规则、最终评分和outbox。没有推送可能是策略正常工作，不能先假定Telegram故障。

## 数据库维护

SQLite状态属于生产数据，不提交Git。定期检查文件增长、WAL、索引计划和 `quick_check`。长期历史统计不要在实时事件循环中执行；使用缓存、增量状态或离线分析。

## 回滚

回滚顺序：

1. 停止扩大故障的配置或新供应商。
2. 恢复上一份0600环境文件。
3. 将 `current` 指回上一版本。
4. 只重启受影响的unit。
5. 验证其他服务PID未变化。
6. 等待恢复去抖并检查outbox无重复发送。
