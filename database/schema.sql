-- =============================================================
-- 创作者情报工作站 V1.0 — SQLite 数据库 Schema (P1 设计)
-- 设计原则（来自任务书）：
--   1. 保留原始采集数据，分析结果不得覆盖原始数据。
--      => 原始表 (creators/works/comments/...) 含 _raw_json 列存原样响应；
--         分析表 (content_analysis/topic_opportunities) 是派生表，只引用、不回写。
--   2. 每条记录可追溯：source / source_url / collected_at / data_status 四件套齐全。
--   3. 事实、推断、建议分开：data_status 区分
--      collected(实测原始) / derived(分析派生) / inferred(推断) / unverified(未验证) / needs_auth(待授权)。
--   4. 主键 / 外键 / 唯一约束防重。
-- 启用外键：应用层执行 PRAGMA foreign_keys = ON;
-- =============================================================

PRAGMA foreign_keys = ON;

-- ---------- 1. creators 创作者主表（稳定身份，不含随时间变化的指标）----------
CREATE TABLE creators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    platform        TEXT    NOT NULL,                 -- douyin/bilibili/xhs/weishi/weibo/...
    platform_uid    TEXT    NOT NULL,                 -- 平台内唯一 ID
    handle          TEXT,                             -- @账号 / 主页路径
    display_name    TEXT,
    bio             TEXT,
    avatar_url      TEXT,
    category        TEXT,                             -- 观察方向（6 个初始方向之一）
    observation_tag TEXT,                             -- 入选理由标签（持续性/知识价值/结构/...）
    first_seen      TEXT,
    last_seen       TEXT,
    -- 可追溯四件套
    source          TEXT,
    source_url      TEXT,
    collected_at    TEXT,                             -- ISO8601
    data_status     TEXT    DEFAULT 'collected',      -- collected/needs_auth/unverified
    _raw_json       TEXT,                             -- 原始响应（原样保留）
    UNIQUE(platform, platform_uid)
);

-- ---------- 2. creator_snapshots 创作者指标快照（时间序列）----------
CREATE TABLE creator_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    creator_id      INTEGER NOT NULL,
    snapshot_at     TEXT    NOT NULL,
    follower_count  INTEGER,
    following_count INTEGER,
    work_count      INTEGER,
    total_likes     INTEGER,
    avg_views       INTEGER,
    raw_metrics_json TEXT,
    source          TEXT,
    source_url      TEXT,
    collected_at    TEXT,
    data_status     TEXT    DEFAULT 'collected',
    FOREIGN KEY (creator_id) REFERENCES creators(id) ON DELETE CASCADE,
    UNIQUE(creator_id, snapshot_at)
);

-- ---------- 3. works 作品主表（稳定元数据）----------
CREATE TABLE works (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    platform        TEXT    NOT NULL,
    work_id         TEXT    NOT NULL,                 -- 平台作品 ID (bvid / aweme_id / note_id ...)
    creator_id      INTEGER,
    title           TEXT,
    publish_time    TEXT,
    url             TEXT,
    cover_url       TEXT,
    duration        INTEGER,
    work_type       TEXT,                             -- video / article / note / ...
    first_seen      TEXT,
    last_seen       TEXT,
    source          TEXT,
    source_url      TEXT,
    collected_at    TEXT,
    data_status     TEXT    DEFAULT 'collected',
    _raw_json       TEXT,
    UNIQUE(platform, work_id),
    FOREIGN KEY (creator_id) REFERENCES creators(id) ON DELETE SET NULL
);

-- ---------- 4. work_snapshots 作品指标快照（时间序列）----------
CREATE TABLE work_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    work_id         INTEGER NOT NULL,
    snapshot_at     TEXT    NOT NULL,
    play_count      INTEGER,
    like_count      INTEGER,
    comment_count   INTEGER,
    share_count     INTEGER,
    collect_count   INTEGER,
    raw_metrics_json TEXT,
    source          TEXT,
    source_url      TEXT,
    collected_at    TEXT,
    data_status     TEXT    DEFAULT 'collected',
    FOREIGN KEY (work_id) REFERENCES works(id) ON DELETE CASCADE,
    UNIQUE(work_id, snapshot_at)
);

-- ---------- 5. comments 评论（按任务书要求脱敏，不存与研究无关的个人信息）----------
CREATE TABLE comments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    platform        TEXT    NOT NULL,
    comment_id      TEXT    NOT NULL,
    work_id         INTEGER,
    creator_id      INTEGER,                          -- 作品所属创作者（便于聚合）
    parent_comment_id TEXT,                           -- 楼中楼回复
    author_handle   TEXT,                             -- 仅公开昵称/账号，不存手机号/实名等 PII
    content         TEXT,
    like_count      INTEGER,
    comment_time    TEXT,
    is_sample       INTEGER DEFAULT 0,                -- 是否被选入需求研究样本 (0/1)
    source          TEXT,
    source_url      TEXT,
    collected_at    TEXT,
    data_status     TEXT    DEFAULT 'collected',
    _raw_json       TEXT,
    UNIQUE(platform, comment_id),
    FOREIGN KEY (work_id)   REFERENCES works(id)     ON DELETE CASCADE,
    FOREIGN KEY (creator_id) REFERENCES creators(id) ON DELETE SET NULL
);

-- ---------- 6. content_analysis 内容分析（派生表，绝不回写原始表）----------
CREATE TABLE content_analysis (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    work_id         INTEGER NOT NULL UNIQUE,          -- 一作品一分析
    analysis_type   TEXT,                             -- deep_deconstruct / quick_tag
    topic           TEXT,                             -- 选题
    title_eval      TEXT,                             -- 标题评估
    hook            TEXT,                             -- 开头钩子
    structure       TEXT,                             -- 内容结构
    core_view       TEXT,                             -- 核心观点
    argumentation   TEXT,                             -- 论证方式
    emotion         TEXT,                             -- 情绪机制
    narrative       TEXT,                             -- 叙事技巧
    raw_analysis_json TEXT,                           -- 分析师完整输出
    analyst         TEXT,                             -- 岗位：创作者分析师
    confidence      TEXT,                             -- high/medium/low
    analysis_at     TEXT,
    source          TEXT,                             -- 通常 = works.source_url（可追溯回原作品）
    source_url      TEXT,
    collected_at    TEXT,
    data_status     TEXT    DEFAULT 'derived',        -- 派生，不是原始采集
    FOREIGN KEY (work_id) REFERENCES works(id) ON DELETE CASCADE
);

-- ---------- 7. trends 趋势（热榜 / 公开趋势入口）----------
CREATE TABLE trends (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    platform        TEXT    NOT NULL,                 -- weibo/baidu/bilibili/...
    trend_source    TEXT,                             -- weibo_hot / baidu_realtime / bili_popular
    rank            INTEGER,
    keyword         TEXT,
    heat_value      INTEGER,
    tag             TEXT,                             -- 分类标签
    trend_time      TEXT,                             -- 观测时刻
    source          TEXT,
    source_url      TEXT,
    collected_at    TEXT,
    data_status     TEXT    DEFAULT 'collected',
    _raw_json       TEXT,
    UNIQUE(platform, trend_source, trend_time, rank)
);

-- ---------- 8. topic_opportunities 选题机会（派生表）----------
CREATE TABLE topic_opportunities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    title           TEXT,                             -- 候选选题
    based_on        TEXT,                             -- 来源类型：user_need / trend / supply_gap
    need_ref        TEXT,                             -- 引用的 comments/content_analysis id 列表（JSON 数组）
    trend_ref       TEXT,                             -- 引用的 trends id 列表
    supply_analysis TEXT,                            -- 内容供给 / 同质化程度
    differentiation TEXT,                            -- 差异化切入空间
    personal_fit    TEXT,                             -- 我的个人表达适配程度
    series_potential TEXT,                            -- 是否可发展为系列内容
    priority        TEXT,                             -- P0 / P1 / P2
    status          TEXT    DEFAULT 'candidate',      -- candidate / selected_for_deep
    raw_json        TEXT,
    planner         TEXT,                             -- 岗位：选题策划师
    created_at      TEXT,
    source          TEXT,
    source_url      TEXT,
    collected_at    TEXT,
    data_status     TEXT    DEFAULT 'derived'
    -- 注：need_ref / trend_ref 存的是 JSON 数组文本（多引用），不能作为单列外键。
    --     原「FOREIGN KEY (need_ref) REFERENCES comments(id)」为设计缺陷（类型不匹配、
    --     且会误约束派生表），已于采集系统落地时移除；多对多引用由应用层 / 关联表维护。
);

-- =============================================================
-- 审计与可追溯性（数据审计员岗位用，建议扩展，不计入上述 8 表）
-- =============================================================
CREATE TABLE collection_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at          TEXT,
    role            TEXT,                             -- 数据采集员 / ...
    source          TEXT,
    target          TEXT,                             -- 采集目标描述
    items_collected INTEGER,
    status          TEXT,                             -- success / partial / blocked / needs_auth
    note            TEXT,
    source_url      TEXT
);

-- ---------- 常用索引 ----------
CREATE INDEX idx_creator_platform ON creators(platform);
CREATE INDEX idx_works_creator   ON works(creator_id);
CREATE INDEX idx_comments_work   ON comments(work_id);
CREATE INDEX idx_trends_time     ON trends(trend_time);
CREATE INDEX idx_analysis_work   ON content_analysis(work_id);
CREATE INDEX idx_topic_status    ON topic_opportunities(status);
