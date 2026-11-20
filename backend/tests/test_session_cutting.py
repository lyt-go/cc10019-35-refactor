"""会话切分重构与策略关闭预演的测试。

覆盖：
1. 重构前后一致：同一输入的会话事件、边界和时间与参照实现相同。
2. 边界诊断：同一对事件同时命中多条切割规则时记录全部命中与优先级生效。
3. 可回放：边界诊断数据支持重放核对。
4. 预演：关闭一条策略后哪些会话断边会连起来，影响范围可核对。
5. 同一基准：预演两侧的事件与规则来自同一份基准。
6. 失败保护：预演失败不覆盖已发布的会话与路径产物。
7. 历史重放：规则与事件变化后，重放仍得出一致的历史预演结果。
"""
from __future__ import annotations

import copy
import os
import sys
import unittest
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.store import store
from app.services.session_cutting import (
    CuttingRules,
    evaluate_boundary,
    fingerprint,
    split_event_stream,
)
from app.services.session_split import (
    _generate_raw_events,
    get_boundary_diagnostics,
    replay_session_boundaries,
    split_sessions,
)
from app.services.behavior_path import (
    build_path_sankey,
    preview_policy_disable,
    replay_preview,
    verify_preview,
)


# ========== 重构前的原始切分逻辑（参照实现，逐行保留原判断顺序） ==========
def _reference_split(
    events: list[dict],
    timeout_sec: int,
    cross_day_policy: str,
    cross_channel_policy: str,
    session_key: str,
) -> tuple[list[dict], list[bool]]:
    keyed_events: dict[str, list[dict]] = defaultdict(list)
    for e in events:
        key = e.get(session_key, e["user_id"])
        keyed_events[key].append(e)

    sessions: list[dict] = []
    split_flags: list[bool] = []

    for key, evts in keyed_events.items():
        evts.sort(key=lambda x: x["timestamp"])
        current_session: list[dict] = []
        session_start = evts[0]["timestamp"] if evts else 0

        for evt in evts:
            if not current_session:
                current_session = [evt]
                session_start = evt["timestamp"]
                continue

            prev = current_session[-1]
            time_diff = evt["timestamp"] - prev["timestamp"]

            should_split = time_diff > timeout_sec

            if not should_split and cross_day_policy == "split":
                prev_day = prev["timestamp"] // 86400
                curr_day = evt["timestamp"] // 86400
                if prev_day != curr_day:
                    should_split = True

            if not should_split and cross_channel_policy == "split":
                if prev.get("channel") != evt.get("channel"):
                    should_split = True

            split_flags.append(should_split)
            if should_split:
                sessions.append({
                    "session_key": key,
                    "events": [dict(e) for e in current_session],
                    "event_count": len(current_session),
                    "start_time": session_start,
                    "end_time": prev["timestamp"],
                    "duration": prev["timestamp"] - session_start,
                })
                current_session = [evt]
                session_start = evt["timestamp"]
            else:
                current_session.append(evt)

        if current_session:
            sessions.append({
                "session_key": key,
                "events": [dict(e) for e in current_session],
                "event_count": len(current_session),
                "start_time": session_start,
                "end_time": current_session[-1]["timestamp"],
                "duration": current_session[-1]["timestamp"] - session_start,
            })

    return sessions, split_flags


def _craft_multi_hit_events() -> list[dict]:
    """构造测试事件流：一对事件同时跨午夜、超时并切换渠道。"""
    return [
        # K1：e1 -> e2 同时命中 timeout + cross_day + cross_channel
        {"user_id": "K1", "device_id": "DA", "event_id": "EV-001",
         "timestamp": 1700002800, "channel": "app", "page_url": "/p1"},
        {"user_id": "K1", "device_id": "DA", "event_id": "EV-002",
         "timestamp": 1700006400, "channel": "web", "page_url": "/p2"},
        # K1：e2 -> e3 不命中任何规则（会话内连边）
        {"user_id": "K1", "device_id": "DA", "event_id": "EV-003",
         "timestamp": 1700006700, "channel": "web", "page_url": "/p3"},
        # K2：f1 -> f2 仅命中 timeout
        {"user_id": "K2", "device_id": "DB", "event_id": "EV-001",
         "timestamp": 1700010000, "channel": "app", "page_url": "/p1"},
        {"user_id": "K2", "device_id": "DB", "event_id": "EV-002",
         "timestamp": 1700013600, "channel": "app", "page_url": "/p2"},
    ]


class TestRefactorParity(unittest.TestCase):
    """重构前后一致：同一输入的会话事件、边界和时间相同。"""

    def _assert_parity(self, events: list[dict], rules: CuttingRules) -> None:
        ref_sessions, ref_flags = _reference_split(
            events,
            rules.timeout_sec,
            rules.cross_day_policy,
            rules.cross_channel_policy,
            rules.session_key,
        )
        outcome = split_event_stream([dict(e) for e in events], rules)
        # 会话事件、边界和时间完全一致
        self.assertEqual(outcome.sessions, ref_sessions)
        self.assertEqual([b["split"] for b in outcome.boundaries], ref_flags)
        self.assertEqual(outcome.total_events, len(events))

    def test_parity_on_generated_stream_ss001(self) -> None:
        rules = CuttingRules(timeout_sec=1800, cross_day_policy="split",
                             cross_channel_policy="split", session_key="user_id")
        self._assert_parity(_generate_raw_events(), rules)

    def test_parity_on_generated_stream_ss002(self) -> None:
        rules = CuttingRules(timeout_sec=3600, cross_day_policy="merge",
                             cross_channel_policy="merge", session_key="device_id")
        self._assert_parity(_generate_raw_events(), rules)

    def test_parity_edge_cases(self) -> None:
        rules = CuttingRules(timeout_sec=1800, cross_day_policy="split",
                             cross_channel_policy="split", session_key="user_id")
        # 空流与单事件
        self._assert_parity([], rules)
        self._assert_parity([{"user_id": "A", "timestamp": 100, "channel": "app"}], rules)
        # 时间差恰好等于阈值：不切割（原逻辑为严格大于）
        events = [
            {"user_id": "A", "timestamp": 1000, "channel": "app"},
            {"user_id": "A", "timestamp": 2800, "channel": "app"},
        ]
        self._assert_parity(events, rules)
        outcome = split_event_stream(events, rules)
        self.assertEqual(len(outcome.sessions), 1)
        # 缺少会话标识字段时回退 user_id
        events = [
            {"user_id": "A", "timestamp": 1000, "channel": "app"},
            {"user_id": "A", "timestamp": 5000, "channel": "app"},
        ]
        self._assert_parity(events, CuttingRules(timeout_sec=1800, session_key="missing_field"))

    def test_parity_split_sessions_result(self) -> None:
        """split_sessions 的统计与前20个会话与参照实现一致。"""
        store._artifacts.clear()
        result = split_sessions("SS-001")
        ref_sessions, _ = _reference_split(
            _generate_raw_events(), 1800, "split", "split", "user_id")
        self.assertEqual(result.session_count, len(ref_sessions))
        self.assertEqual(result.total_events, len(_generate_raw_events()))
        avg = sum(s["duration"] for s in ref_sessions) / len(ref_sessions)
        self.assertEqual(result.avg_session_duration, round(avg, 2))
        self.assertEqual(result.sessions, ref_sessions[:20])


class TestBoundaryDiagnostics(unittest.TestCase):
    """边界诊断：多规则命中、优先级与可回放。"""

    def test_multi_rule_hit_records_all_and_priority(self) -> None:
        """同一对事件既跨午夜又超时并切换渠道：记录全部命中，超时优先生效。"""
        rules = CuttingRules(timeout_sec=1800, cross_day_policy="split",
                             cross_channel_policy="split", session_key="user_id")
        prev = {"user_id": "K1", "timestamp": 1700002800, "channel": "app"}
        curr = {"user_id": "K1", "timestamp": 1700006400, "channel": "web"}
        decision = evaluate_boundary(prev, curr, rules)
        self.assertEqual(decision.time_diff, 3600)
        self.assertEqual(set(decision.hit_rules), {"timeout", "cross_day", "cross_channel"})
        self.assertEqual(decision.applied_rule, "timeout")
        self.assertTrue(decision.split)

    def test_priority_order_matches_legacy(self) -> None:
        """未命中超时时按 跨天 -> 跨渠道 顺序生效。"""
        rules = CuttingRules(timeout_sec=1800, cross_day_policy="split",
                             cross_channel_policy="split", session_key="user_id")
        prev = {"user_id": "K1", "timestamp": 1700006100, "channel": "app"}
        curr = {"user_id": "K1", "timestamp": 1700006400, "channel": "web"}
        decision = evaluate_boundary(prev, curr, rules)
        self.assertEqual(set(decision.hit_rules), {"cross_day", "cross_channel"})
        self.assertEqual(decision.applied_rule, "cross_day")

    def test_diagnostics_artifact_replayable(self) -> None:
        """边界诊断产物包含同一基准的事件与规则，可重放核对。"""
        store._artifacts.clear()
        split_sessions("SS-001")
        diag = get_boundary_diagnostics("SS-001")
        self.assertIsNotNone(diag)
        self.assertEqual(diag["fingerprint"], fingerprint(diag["events"], diag["rules"]))
        self.assertGreater(len(diag["boundaries"]), 0)
        for b in diag["boundaries"]:
            self.assertIn("hit_rules", b)
            self.assertIn("applied_rule", b)
            self.assertIn("split", b)

        report = replay_session_boundaries("SS-001")
        self.assertTrue(report["consistent"])
        self.assertTrue(all(c["match"] for c in report["checks"].values()))


class TestPreviewPolicyDisable(unittest.TestCase):
    """预演关闭切割策略：影响范围、同一基准、失败保护与历史重放。"""

    STRATEGY = {
        "id": 901, "status": "已生效", "pending": False, "abnormal": False,
        "strategy_id": "SS-TEST", "strategy_name": "预演测试策略",
        "timeout_sec": 1800, "cross_day_policy": "split",
        "cross_channel_policy": "split", "attribution_window": 604800,
        "session_key": "user_id", "version": "v1.0",
    }
    MODEL = {
        "id": 902, "status": "已生效", "pending": False, "abnormal": False,
        "model_id": "BP-TEST", "start_event": "EV-001", "target_event": "EV-003",
        "time_window": 86400, "path_depth": 5, "session_split_rule": "SS-TEST",
        "path_filter": "", "analysis_range": "2026-11-01/2026-11-15",
    }

    def setUp(self) -> None:
        store._artifacts.clear()
        store.insert("session_split", dict(self.STRATEGY))
        store.insert("behavior_path", dict(self.MODEL))
        # 发布同一基准的边界诊断产物
        split_sessions("SS-TEST", events=_craft_multi_hit_events())

    def tearDown(self) -> None:
        store.delete("session_split", self.STRATEGY["id"])
        store.delete("behavior_path", self.MODEL["id"])
        store._artifacts.clear()

    def test_preview_impact_scope_is_auditable(self) -> None:
        """关闭超时策略：仅命中超时的断边连起来，多规则命中的断边仍断开。"""
        preview = preview_policy_disable("BP-TEST", "timeout")
        impact = preview["impact"]

        # 基准：4个会话、2条断边；预演：3个会话、1条断边
        self.assertEqual(impact["baseline_sessions"], 4)
        self.assertEqual(impact["scenario_sessions"], 3)
        self.assertEqual(impact["baseline_broken_edges"], 2)
        self.assertEqual(impact["scenario_broken_edges"], 1)

        # K2 的边仅命中超时 -> 连起来
        self.assertEqual(len(impact["reconnected_edges"]), 1)
        reconnected = impact["reconnected_edges"][0]
        self.assertEqual(reconnected["session_key"], "K2")
        self.assertEqual(reconnected["baseline_hit_rules"], ["timeout"])
        self.assertFalse(reconnected["scenario_split"])

        # K1 的边同时命中三条规则 -> 关闭超时后仍被跨天顶住
        self.assertEqual(len(impact["still_broken_edges"]), 1)
        still = impact["still_broken_edges"][0]
        self.assertEqual(still["session_key"], "K1")
        self.assertEqual(set(still["baseline_hit_rules"]),
                         {"timeout", "cross_day", "cross_channel"})
        self.assertEqual(still["baseline_applied_rule"], "timeout")
        self.assertEqual(still["scenario_applied_rule"], "cross_day")
        self.assertTrue(still["scenario_split"])
        self.assertEqual(impact["still_broken_by_rule"], {"cross_day": 1})
        self.assertEqual(impact["newly_broken_edges"], [])

    def test_preview_uses_single_baseline(self) -> None:
        """预演两侧的事件与规则来自同一基准（已发布的诊断产物）。"""
        preview = preview_policy_disable("BP-TEST", "timeout")
        baseline = preview["baseline"]
        self.assertEqual(baseline["source"], "published")
        published = store.get_artifact("session_boundaries", "SS-TEST")
        self.assertEqual(baseline["fingerprint"], published["fingerprint"])
        self.assertEqual(baseline["events"], published["events"])
        self.assertEqual(baseline["rules"], published["rules"])
        self.assertEqual(
            baseline["fingerprint"],
            fingerprint(baseline["events"], baseline["rules"]),
        )

    def test_verify_preview_against_actual_broken_edges(self) -> None:
        """用同一份历史事件实际重切，核对预演的断边变化。"""
        preview_policy_disable("BP-TEST", "timeout")
        report = verify_preview("BP-TEST", "timeout")
        self.assertTrue(report["verified"])
        checks = report["checks"]
        self.assertTrue(checks["scenario_broken_edges"]["match"])
        self.assertTrue(checks["baseline_broken_edges"]["match"])
        self.assertTrue(checks["reconnected_edges"]["match"])
        # 实际断边只剩 K1 的多规则命中边
        self.assertEqual(len(checks["scenario_broken_edges"]["actual"]), 1)

    def test_preview_failure_keeps_published_artifacts(self) -> None:
        """预演失败不覆盖已发布的会话与路径产物。"""
        build_path_sankey("BP-TEST")
        session_before = copy.deepcopy(store.get_artifact("session_split", "SS-TEST"))
        boundaries_before = copy.deepcopy(store.get_artifact("session_boundaries", "SS-TEST"))
        path_before = copy.deepcopy(store.get_artifact("behavior_path", "BP-TEST"))
        self.assertIsNotNone(session_before)
        self.assertIsNotNone(path_before)

        # 未知规则
        with self.assertRaises(ValueError):
            preview_policy_disable("BP-TEST", "bogus_rule")
        # 不存在的模型
        with self.assertRaises(ValueError):
            preview_policy_disable("BP-NONE", "timeout")
        # 不存在的策略
        store.update("behavior_path", self.MODEL["id"], {"session_split_rule": "SS-NONE"})
        with self.assertRaises(ValueError):
            preview_policy_disable("BP-TEST", "timeout")
        store.update("behavior_path", self.MODEL["id"], {"session_split_rule": "SS-TEST"})

        self.assertEqual(store.get_artifact("session_split", "SS-TEST"), session_before)
        self.assertEqual(store.get_artifact("session_boundaries", "SS-TEST"), boundaries_before)
        self.assertEqual(store.get_artifact("behavior_path", "BP-TEST"), path_before)

    def test_successful_preview_does_not_touch_published_artifacts(self) -> None:
        """预演成功也只写预演产物，不改已发布产物。"""
        build_path_sankey("BP-TEST")
        session_before = copy.deepcopy(store.get_artifact("session_split", "SS-TEST"))
        boundaries_before = copy.deepcopy(store.get_artifact("session_boundaries", "SS-TEST"))
        path_before = copy.deepcopy(store.get_artifact("behavior_path", "BP-TEST"))

        preview_policy_disable("BP-TEST", "timeout")

        self.assertEqual(store.get_artifact("session_split", "SS-TEST"), session_before)
        self.assertEqual(store.get_artifact("session_boundaries", "SS-TEST"), boundaries_before)
        self.assertEqual(store.get_artifact("behavior_path", "BP-TEST"), path_before)
        self.assertIsNotNone(store.get_artifact("path_preview", "BP-TEST:SS-TEST:timeout"))

    def test_historical_replay_consistent_after_rules_and_events_change(self) -> None:
        """规则与事件变化后，历史重放仍得出与预演一致的结果。"""
        preview = preview_policy_disable("BP-TEST", "timeout")

        # 规则变化：调高超时阈值；事件变化：用另一份事件重新切割并发布
        store.update("session_split", self.STRATEGY["id"], {"timeout_sec": 99999})
        changed_events = [
            {"user_id": "K9", "device_id": "DZ", "event_id": "EV-001",
             "timestamp": 1700100000, "channel": "app", "page_url": "/p1"},
            {"user_id": "K9", "device_id": "DZ", "event_id": "EV-002",
             "timestamp": 1700100300, "channel": "app", "page_url": "/p2"},
        ]
        split_sessions("SS-TEST", events=changed_events)
        # 发布的诊断产物已被新基准覆盖
        published = store.get_artifact("session_boundaries", "SS-TEST")
        self.assertNotEqual(published["fingerprint"], preview["baseline"]["fingerprint"])

        # 历史重放仍与当初的预演一致
        report = replay_preview("BP-TEST", "timeout")
        self.assertTrue(report["consistent"])
        self.assertEqual(report["baseline_fingerprint"], preview["baseline"]["fingerprint"])
        self.assertEqual(report["recomputed_impact"], preview["impact"])

        # 佐证：若误用新基准重算，结果会不同（重放确实用了历史基准）
        from app.services.behavior_path import _compute_impact
        new_impact = _compute_impact(published["events"], published["rules"], "timeout")
        self.assertNotEqual(new_impact, preview["impact"])

    def test_preview_without_published_baseline_uses_ephemeral(self) -> None:
        """未发布诊断产物时，预演用当前策略与事件临时构建同一基准。"""
        store._artifacts.clear()
        preview = preview_policy_disable("BP-TEST", "timeout")
        self.assertEqual(preview["baseline"]["source"], "ephemeral")
        # 临时基准同样可核对、可重放
        self.assertTrue(verify_preview("BP-TEST", "timeout")["verified"])
        self.assertTrue(replay_preview("BP-TEST", "timeout")["consistent"])
        # 预演没有顺手发布会话产物
        self.assertIsNone(store.get_artifact("session_split", "SS-TEST"))
        self.assertIsNone(store.get_artifact("session_boundaries", "SS-TEST"))


class TestPreviewOnSeedData(unittest.TestCase):
    """基于种子数据的预演：SS-002 关闭超时后所有断边连起来。"""

    MODEL = {
        "id": 903, "status": "已生效", "pending": False, "abnormal": False,
        "model_id": "BP-TEST2", "start_event": "EV-001", "target_event": "EV-003",
        "time_window": 86400, "path_depth": 5, "session_split_rule": "SS-002",
        "path_filter": "", "analysis_range": "2026-11-01/2026-11-15",
    }

    def setUp(self) -> None:
        store._artifacts.clear()
        store.insert("behavior_path", dict(self.MODEL))

    def tearDown(self) -> None:
        store.delete("behavior_path", self.MODEL["id"])
        store._artifacts.clear()

    def test_disable_timeout_reconnects_all_broken_edges(self) -> None:
        preview = preview_policy_disable("BP-TEST2", "timeout")
        impact = preview["impact"]
        # SS-002 只有超时一条生效规则，关闭后不再有断边
        self.assertGreater(impact["baseline_broken_edges"], 0)
        self.assertEqual(impact["scenario_broken_edges"], 0)
        self.assertEqual(len(impact["still_broken_edges"]), 0)
        self.assertEqual(
            len(impact["reconnected_edges"]), impact["baseline_broken_edges"])
        self.assertTrue(verify_preview("BP-TEST2", "timeout")["verified"])
        self.assertTrue(replay_preview("BP-TEST2", "timeout")["consistent"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
