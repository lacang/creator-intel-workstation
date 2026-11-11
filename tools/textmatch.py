#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""话题文本匹配：精确归一化 + 受控模糊共振。

背景（第二轮审计 A3 的实测结论）：
  原逻辑用「整句去标点后完全相等」判定跨平台共振。末轮 152 条热点里
  只命中 5 组，且全部是微博+百度（两家措辞接近），**抖音 0 参与**。
  根因是各平台对同一事件的措辞不同，例如：
    抖音「凯尔特人大胜76人」 / 微博「凯尔特人战胜76人」 / 百度「凯尔特人大胜76人」
  整句相等过严，导致「跨平台共振」这个最重要的情报信号几乎失效。

本模块的策略（可解释、可控、不引入任何外部依赖）：
  1. exact：归一化后完全相等，置信度高；
  2. fuzzy ：共享一个「信息量足够的 4 字连续片段」（shingle），
            例如「凯尔特人」「梁家辉」「初雪」；要求片段含 >=3 个汉字、
            不是通用功能词、且最多连接 2 个精确组（防止一个热字把全榜连成一片）。

重要的诚实性约束：
  模糊匹配是**推断**，不是平台给出的事实。所有 fuzzy 簇都带 evidence
  （命中的公共片段）与 match 标记；调用方必须在看板 / 派生表里显式标注，
  不得与 exact 混为一谈、不得据此声称「平台确认同一事件」。
"""
from __future__ import annotations

import re
from collections import defaultdict

# 归一化：去掉所有空白与标点，保留中英文与数字，统一小写
_PUNCT = re.compile(r"[\s\W_]+", re.UNICODE)
_CJK = re.compile(r"[\u4e00-\u9fff]")

# 纯功能字 / 营销套话：由这些字组成的片段不具区分度，不能作为共振证据
_FUNC_CHARS = set("的了是在有和与被把就都还也很最更上下中前后新老大小多少"
                  "网友网曝曝回应通报最新引发热议登上热搜视频官方消息今天昨日"
                  "现场直播报道公布宣布表示称说说道这那其之为对从到及等")
_BAD_SHINGLES = {
    "网友回应", "官方通报", "引发热议", "登上热搜", "最新消息", "现场视频",
    "相关视频", "正式发布", "正式官宣", "首次曝光", "全网热议", "冲上热搜",
    "当事人回应", "工作人员回应", "据悉", "目前", "已经", "什么", "如何",
}


def norm(s) -> str:
    """整句归一化：去标点空白 + 小写。用于精确匹配与 movements 跨轮对比。"""
    return _PUNCT.sub("", (s or "")).lower()


def _informative(g: str) -> bool:
    """片段是否具备区分度：至少 3 个汉字、不是套话、不是纯功能字。"""
    if len(_CJK.findall(g)) < 3:
        return False
    if g in _BAD_SHINGLES:
        return False
    # 至少 2 个字不是功能字，避免「大家都」「这个人」之类
    if sum(1 for c in g if c not in _FUNC_CHARS) < 2:
        return False
    return True


def shingles(s, k: int = 4) -> set[str]:
    """取归一化串的所有 k 字连续片段，只保留有信息量的。"""
    n = norm(s)
    if len(n) < k:
        return set()
    out = set()
    for i in range(len(n) - k + 1):
        g = n[i:i + k]
        if _informative(g):
            out.add(g)
    return out


# ---------------------------------------------------------------- 并查集
class _DSU:
    def __init__(self, keys):
        self.p = {k: k for k in keys}

    def find(self, x):
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def resonance(records: list, min_platforms: int = 2, max_clusters: int = 40):
    """把一轮热榜按「精确相等 + 受控模糊共现」聚成跨平台话题簇。

    参数 records: [{'platform','keyword','rank','heat_value', ...}, ...]
    返回: [{'keyword','platforms','best_rank','max_heat','match','evidence',
            'platform_count','members'}, ...]  按 平台数 → 最佳榜位 排序。

    match = 'exact'（整句相等）/ 'fuzzy'（共享片段，属推断）。
    """
    # 1) 精确归一化分组
    groups: dict[str, dict] = {}
    for r in records:
        k = norm(r.get("keyword"))
        if not k:
            continue
        g = groups.setdefault(k, {
            "key": k, "display": r.get("keyword"), "platforms": set(),
            "best_rank": 10 ** 6, "max_heat": 0, "members": [],
        })
        g["platforms"].add(r.get("platform"))
        g["max_heat"] = max(g["max_heat"], r.get("heat_value") or 0)
        rk = r.get("rank")
        if rk:
            g["best_rank"] = min(g["best_rank"], rk)
        g["members"].append({"id": r.get("id"), "platform": r.get("platform"),
                             "keyword": r.get("keyword"), "rank": rk})
        # 展示词取榜位最好的那条，避免用边缘平台的长尾措辞做标题
        if rk and rk == g["best_rank"]:
            g["display"] = r.get("keyword")

    keys = list(groups)
    dsu = _DSU(keys)

    # 2) 模糊：共享 4 字片段的组互相连接。
    #    每个片段最多连接 2 个组；超过说明该片段太通用（如「金鸡奖」），丢弃。
    inv: dict[str, set] = defaultdict(set)
    evidence: dict[frozenset, str] = {}
    for k, g in groups.items():
        for sh in shingles(g["display"]):
            inv[sh].add(k)
    for sh, ks in inv.items():
        if len(ks) != 2:
            continue
        a, b = tuple(ks)
        # 同平台内合并意义不大，只保留能带来「跨平台」信息的连接
        if groups[a]["platforms"] == groups[b]["platforms"] and \
           len(groups[a]["platforms"] | groups[b]["platforms"]) < 2:
            continue
        dsu.union(a, b)
        evidence[frozenset((a, b))] = sh

    # 3) 汇总簇
    clusters: dict[str, list] = defaultdict(list)
    for k in keys:
        clusters[dsu.find(k)].append(k)

    out = []
    for ks in clusters.values():
        plats = set()
        best, heat, members = 10 ** 6, 0, []
        for k in ks:
            g = groups[k]
            plats |= g["platforms"]
            best = min(best, g["best_rank"])
            heat = max(heat, g["max_heat"])
            members.extend(g["members"])
        if len(plats) < min_platforms:
            continue
        # 判定该簇是纯精确还是含模糊合并
        fuzzy_evidence = [evidence[frozenset((a, b))]
                          for a in ks for b in ks
                          if a < b and frozenset((a, b)) in evidence]
        match = "fuzzy" if len(ks) > 1 and fuzzy_evidence else "exact"
        display = min((groups[k]["display"] for k in ks),
                      key=lambda s: len(s)) if len(ks) > 1 else groups[ks[0]]["display"]
        out.append({
            "keyword": display,
            "platforms": sorted(plats),
            "platform_count": len(plats),
            "best_rank": None if best == 10 ** 6 else best,
            "max_heat": heat,
            "match": match,
            "evidence": sorted(set(fuzzy_evidence))[:3] if match == "fuzzy" else [],
            "members": members,
            "exact_groups": len(ks),
        })
    # 共振平台数优先，其次最佳榜位；同分优先 exact（更可信）
    out.sort(key=lambda x: (-x["platform_count"],
                            x["best_rank"] or 999,
                            0 if x["match"] == "exact" else 1))
    return out[:max_clusters]


def keyword_hit(keyword: str, text: str) -> bool:
    """关注词命中：短词做包含匹配；对英文词做大小写不敏感匹配。"""
    k = (keyword or "").strip().lower()
    if not k:
        return False
    return k in (text or "").lower()
