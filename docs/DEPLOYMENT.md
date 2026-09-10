# 部署指南

## 1. 本地验收

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
  python -m unittest discover -s tests -p 'test_*.py'
```

## 2. 运行身份与目录

建议使用独立、无登录权限的系统用户，并将代码、状态与秘密分开：

- 代码：版本目录加原子 `current` 链接；
- 状态：独立SQLite和health目录；
- 秘密：root或服务用户可读的0600环境文件；
- unit：参考 `deploy/systemd/`，根据本机路径调整。

不要把仓库目录同时当作生产状态目录。

## 3. 配置

```bash
cp .env.example .env
chmod 600 .env
```

必要配置按启用的数据源选择。Telegram必须显式开启；默认保持关闭。RPC端点使用完整HTTP/WSS配对，Pons公共轮询可通过以下字段控制：

```text
PONS_PUBLIC_POLL_RPC_URL=<public read-only endpoint>
PONS_PUBLIC_POLL_PRIMARY=1
```

任何环境文件都不得提交到Git。

## 4. 分层上线

1. 使用新的临时数据库运行DRY_RUN。
2. 检查解析错误、checkpoint、数据源延迟和内存。
3. 验证硬门原因和候选数量。
4. 发送一条明确标记的Telegram测试消息。
5. 才允许开启真实强信号outbox。

不要用放宽安全门的方式验证Telegram通道。

## 5. 原子发布

建议为每次发布生成只读目录和SHA-256摘要：

```text
releases/<UTC timestamp>-<release name>/
current -> releases/<selected release>/
```

切换前执行测试；切换后验证服务PID、重启计数、health新鲜度、SQLite `quick_check` 和outbox状态。保留上一个版本与配置备份，以便快速回滚。

## 6. systemd沙箱

示例unit启用：

- `NoNewPrivileges`
- `ProtectSystem=strict`
- 空能力集
- 受限地址族
- 明确的读写目录
- 内存、任务和文件描述符上限
- `UMask=0077`

部署前应逐项核对，不要因为示例能启动就删除沙箱限制。
