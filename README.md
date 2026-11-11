# 创作者情报工作站

14 个免登录公开数据源 → 分级采集 → SQLite（原始/派生分离）→ 实时看板 + 离线快照。

给单人内容账号用的选题情报系统：每 2 分钟扫一遍全网热榜和垂类作品，把「现在什么在涨」收敛成「你能拍什么」。

> 完整架构、模块职责、数据库口径、自愈机制见 **[`00_项目管理/系统架构与运维.md`](00_项目管理/系统架构与运维.md)**，改代码前先读它。

## 快速开始

```bash
pip install -r requirements.txt

python3 tools/collector.py --once      # 手工跑一轮采集
python3 tools/audit.py                 # 19 项只读审计（首选自检）
python3 tools/verify.py                # 数据质量自检 → logs/quality.json
python3 tools/dashboard.py --port 8765 # 前台起实时看板
python3 tools/dashboard.py --export    # 导离线快照 创作者情报看板.html
```

首次运行会自动建库（`database/workstation.db`，WAL 模式，12 表 13 索引），无需手工执行 `schema.sql`。

## 目录

```
tools/          collector 编排入库 · sources_ext 数据源适配器 · dashboard 看板
                textmatch 话题归一化 · maintain 自愈调度 · audit/verify 自检
config/         settings 源开关与节流 · keywords 24 个赛道词 · creator_profile 账号画像
database/       schema.sql（库结构，入库） / 设计方案（库本身不入库）
00_项目管理/     架构运维手册 · 任务追踪 · 审计报告 · 账号规划
01_数据源/       官方渠道调研 · 数据源测试报告
```

## 部署形态

这是**常驻服务**，不是静态站，GitHub Pages 跑不了它。生产部署需要一台常驻机器：

```bash
# cron 每 2 分钟驱动一轮维护（采集→算健康→自检→导快照→清raw→拉服务）
*/2 * * * * /usr/local/bin/ws_maintain.sh
@reboot     /usr/local/bin/ws_maintain.sh
```

`maintain.py` 每步独立 try/except，任一层挂掉不连带其余步骤；看板 `/api/version` 暴露代码指纹，避免「改了代码服务还在跑旧版」。

**只想看数据不想跑服务**：把 `dashboard.py --export` 生成的 `创作者情报看板.html`（数据全内联的单文件）发到任意静态托管即可，不需要后端。

## 数据口径（最容易搞错的三处）

- `trends.heat_available=0` 表示**平台不公开热度**（网易/知乎），不是 0 度
- `collection_runs.status` 里 `scheduled_skip` 是**正常跳过**，不是失败
- `content_analysis` / `topic_opportunities` 是规则引擎派生、`confidence=low`，**必须人工二次筛选**

## 合规红线（不可协商）

仅请求免登录、公开可访问的官方榜单/热点接口。不带登录态或 cookie，不生成伪造签名（`a_bogus` / `X-Bogus` / WBI），不绕过验证码与设备校验，不采集含实名/手机号的 PII。所有入库记录带 `source / source_url / collected_at / data_status` 四件套。

需要授权的数据源一律不接。

## 不入库的东西

`config/*cookie*`、`browser_profile/`、`*.db`、`**/raw/*.json`、`logs/`、`*.png`、`创作者情报看板.html`。凭证和运行产物只留本地，`schema.sql` 入库、库本身由 collector 重建。
