import json
import struct
from typing import Dict, List, Optional, Sequence, Tuple

import fdb

fdb.api_version(710)

_META_FMT = "<i i B"
# window (uint32), value (float32), flags (uint8)
_VALUE_FMT = "<I f B"
_LAST_FMT = "<q d"
_PREFIX_VALUE = b"\x01"
_PREFIX_META = b"\x02"
_PREFIX_META_INFO = b"\x04"


def _pack_meta(step: int, slots: int, typ: int) -> bytes:
    return struct.pack(_META_FMT, step, slots, typ)


def _unpack_meta(raw: bytes) -> Tuple[int, int, int]:
    return struct.unpack(_META_FMT, raw)


def _pack_value_record(window: int, value: float, flags: int) -> bytes:
    return struct.pack(_VALUE_FMT, window, value, flags)


def _pack_last_record(ts: int, raw_value: float) -> bytes:
    return struct.pack(_LAST_FMT, ts, raw_value)


class FdbTsdb:
    FLAG_VALID = 0x1

    def __init__(self, db: fdb.Database, default_step: int, default_slots: int):
        if default_step <= 0 or default_slots <= 0:
            raise ValueError("step and slots must be positive")
        self.db = db
        self.default_step = default_step
        self.default_slots = default_slots

    def _ensure_u32(self, metric_id: int) -> None:
        if metric_id < 0 or metric_id > 0xFFFFFFFF:
            raise ValueError("metric_id must fit in uint32")

    def ensure_metric(
        self,
        metric_id: int,
        typ: int,
        step: Optional[int] = None,
        slots: Optional[int] = None,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        self._ensure_u32(metric_id)
        step = step or self.default_step
        slots = slots or self.default_slots
        if step <= 0 or slots <= 0:
            raise ValueError("step and slots must be positive")
        meta_key = self._meta_key(metric_id)
        meta_bytes = _pack_meta(step, slots, typ)

        def txn(tr: fdb.Transaction) -> None:
            existing = tr.get(meta_key)
            if existing.present():
                current_step, current_slots, current_typ = _unpack_meta(existing.value)
                if (
                    current_step != step
                    or current_slots != slots
                    or current_typ != typ
                ):
                    raise ValueError(
                        "metric %s already registered with different metadata"
                        % metric_id
                    )
            else:
                tr.set(meta_key, meta_bytes)
            self._ensure_meta_info(tr, metric_id, name=name, tags=tags)
            self._ensure_descriptor(tr, metric_id, name=name, tags=tags, typ=typ, step=step, slots=slots)

        self._run_transaction(txn)

    def write_gauge(
        self,
        metric_id: Optional[int],
        ts: int,
        value: float,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        step: Optional[int] = None,
        slots: Optional[int] = None,
    ) -> int:
        metric_id = self.ensure_metric_descriptor(metric_id, typ=0, name=name, tags=tags, step=step, slots=slots)
        self._write_value(metric_id, ts, value)
        return metric_id

    def write_counter(
        self,
        metric_id: Optional[int],
        ts: int,
        raw_value: float,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        step: Optional[int] = None,
        slots: Optional[int] = None,
    ) -> int:
        metric_id = self.ensure_metric_descriptor(
            metric_id, typ=1, name=name, tags=tags, step=step, slots=slots
        )

        def txn(tr: fdb.Transaction) -> None:
            meta = self._load_meta_tr(tr, metric_id)
            if meta is None:
                raise ValueError("metric not found")
            step_val, slots_val, _typ = meta
            slot = self._slot_for(ts, step_val, slots_val)
            window = ts // step_val
            packed_value = _pack_value_record(window, raw_value, self.FLAG_VALID)
            tr.set(self._value_key(metric_id, slot), packed_value)

        self._run_transaction(txn)
        return metric_id

    def read_range(
        self, metric_id: int, start_ts: int, end_ts: int
    ) -> List[Tuple[int, float, int]]:
        if end_ts < start_ts:
            return []
        meta = self._load_meta(metric_id)
        if meta is None:
            return []
        step, slots, typ = meta
        start_window = start_ts // step
        end_window = end_ts // step
        if end_window < start_window:
            return []
        slot_count = min(slots, end_window - start_window + 1)
        if slot_count <= 0:
            return []
        start_slot = start_window % slots
        segments = self._segments_for(start_slot, slot_count, slots)

        def txn(tr: fdb.Transaction) -> List[Tuple[int, float, int]]:
            return self._scan_segments(tr, metric_id, segments, start_ts, end_ts, typ, step)

        return self._run_transaction(txn)

    # Helpers
    def list_metrics(self) -> List[Dict[str, object]]:
        rows = self.db.get_range_startswith(_PREFIX_META, limit=10_000)
        metrics = []
        for kv in rows:
            metric_id = int.from_bytes(kv.key[len(_PREFIX_META) : len(_PREFIX_META) + 4], "big")
            meta = self._load_meta(metric_id)
            if meta is None:
                continue
            step, slots, typ = meta
            info = self._load_meta_info(metric_id)
            metrics.append(
                {
                    "metric_id": metric_id,
                    "name": info.get("name", "") if info else "",
                    "tags": info.get("tags", {}) if info else {},
                    "type": typ,
                    "step": step,
                    "slots": slots,
                }
            )
        metrics.sort(key=lambda m: m["metric_id"])
        return metrics

    def find_metrics(
        self,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        limit: Optional[int] = None,
        return_hit_limit: bool = False,
    ) -> List[Dict[str, object]] | Tuple[List[Dict[str, object]], bool]:
        tags = tags or {}
        results = []
        hit_limit = False
        for metric in self.list_metrics():
            if name and metric.get("name") != name:
                continue
            metric_tags = metric.get("tags", {}) or {}
            if any(metric_tags.get(k) != v for k, v in tags.items()):
                continue
            results.append(metric)
            if limit and len(results) >= limit:
                hit_limit = True
                break
        if return_hit_limit:
            return results, hit_limit
        return results

    def list_metric_names(self, limit: int = 1000) -> List[str]:
        """Return distinct metric names (empty names skipped)."""
        names: List[str] = []
        seen = set()
        for metric in self.list_metrics():
            n = metric.get("name")
            if not n or n in seen:
                continue
            seen.add(n)
            names.append(n)
            if len(names) >= limit:
                break
        names.sort()
        return names

    def tag_catalog(self, name: Optional[str] = None, limit: int = 1000) -> Dict[str, List[str]]:
        """
        Build a tag catalog (key -> list of values) optionally scoped to a metric name.
        Intended for quick UI hints; not guaranteed to be exhaustive at large scale.
        """
        catalog: Dict[str, set] = {}
        count = 0
        for metric in self.list_metrics():
            if name and metric.get("name") != name:
                continue
            tags = metric.get("tags", {}) or {}
            for k, v in tags.items():
                catalog.setdefault(k, set()).add(v)
            count += 1
            if count >= limit:
                break
        return {k: sorted(list(vals)) for k, vals in catalog.items()}

    # Dashboard persistence
    def save_dashboard(self, slug: str, title: str, definition: Dict[str, object]) -> None:
        if not slug:
            raise ValueError("slug is required")
        payload = {"title": title, "definition": definition}
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        key = self._dashboard_key(slug)
        self._run_transaction(lambda tr: tr.set(key, encoded))

    def delete_dashboard(self, slug: str) -> None:
        if not slug:
            return
        key = self._dashboard_key(slug)
        self._run_transaction(lambda tr: tr.clear(key))

    def list_dashboards(self) -> List[Dict[str, str]]:
        prefix = self._dashboard_key_prefix()
        rows = self.db.get_range_startswith(prefix, limit=1000)
        dashboards = []
        for kv in rows:
            _, slug = fdb.tuple.unpack(kv.key)
            try:
                payload = json.loads(kv.value.decode("utf-8"))
            except Exception:
                payload = {}
            dashboards.append({"slug": slug, "title": payload.get("title", slug)})
        dashboards.sort(key=lambda d: d["slug"])
        return dashboards

    def get_dashboard(self, slug: str) -> Optional[Dict[str, object]]:
        key = self._dashboard_key(slug)

        def txn(tr: fdb.Transaction) -> Optional[Dict[str, object]]:
            raw = tr.get(key)
            if not raw.present():
                return None
            try:
                return json.loads(raw.value.decode("utf-8"))
            except Exception:
                return None

        return self._run_transaction(txn)

    def ensure_metric_descriptor(
        self,
        metric_id: Optional[int],
        typ: int,
        step: Optional[int] = None,
        slots: Optional[int] = None,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> int:
        if metric_id is not None:
            self._ensure_u32(metric_id)
        step = step or self.default_step
        slots = slots or self.default_slots
        tags = tags or {}
        name = name or ""
        if metric_id is None and not name:
            raise ValueError("metric_id or name must be provided")

        def txn(tr: fdb.Transaction) -> int:
            if name:
                existing_id = self._lookup_descriptor_tr(tr, name, tags)
                if existing_id is not None:
                    meta = self._load_meta_tr(tr, existing_id)
                    if meta and meta[2] != typ:
                        raise ValueError(
                            f"metric {existing_id} already registered with different type"
                        )
                    self._ensure_meta_info(tr, existing_id, name=name, tags=tags)
                    return existing_id

            mid = metric_id
            if mid is None:
                mid = self._allocate_metric_id(tr)
            meta_key = self._meta_key(mid)
            meta_bytes = _pack_meta(step, slots, typ)
            existing = tr.get(meta_key)
            if existing.present():
                current_step, current_slots, current_typ = _unpack_meta(existing.value)
                if (
                    current_step != step
                    or current_slots != slots
                    or current_typ != typ
                ):
                    raise ValueError(
                        "metric %s already registered with different metadata"
                        % mid
                    )
            else:
                tr.set(meta_key, meta_bytes)
            self._ensure_meta_info(tr, mid, name=name or None, tags=tags)
            self._ensure_descriptor(tr, mid, name=name or None, tags=tags, typ=typ, step=step, slots=slots)
            return mid

        return self._run_transaction(txn)

    def delete_metric(self, metric_id: int) -> None:
        info = self._load_meta_info(metric_id) or {}
        name = info.get("name")
        tags = info.get("tags") or {}

        def txn(tr: fdb.Transaction) -> None:
            meta = self._load_meta_tr(tr, metric_id)
            if meta is None:
                return
            # Clear values
            value_prefix = _PREFIX_VALUE + metric_id.to_bytes(4, "big")
            tr.clear_range_startswith(value_prefix)
            # Clear last value
            tr.clear(self._last_key(metric_id))
            # Clear meta and descriptor
            tr.clear(self._meta_key(metric_id))
            tr.clear(self._meta_info_key(metric_id))
            if name:
                tr.clear(self._descriptor_key(name, tags))

        self._run_transaction(txn)

    def rewrite_metric_retention(self, metric_id: int, step: int, slots: int) -> None:
        if step <= 0 or slots <= 0:
            raise ValueError("step and slots must be positive")
        meta = self._load_meta(metric_id)
        info = self._load_meta_info(metric_id) or {}
        if meta is None:
            raise ValueError("metric not found")
        old_step, old_slots, typ = meta
        if typ != 0:
            raise ValueError("retention rewrite only supported for gauge metrics")
        name = info.get("name")
        tags = info.get("tags") or {}
        # Read existing data before rewriting.
        rows = self.read_range(metric_id, 0, 2**63 - 1)
        # Delete and recreate descriptor with new retention.
        self.delete_metric(metric_id)
        self.ensure_metric_descriptor(metric_id, typ=typ, step=step, slots=slots, name=name, tags=tags)
        # Replay values.
        for ts, val, _ in rows:
            self._write_value(metric_id, ts, val)

    def _write_value(self, metric_id: int, ts: int, value: float) -> None:
        def txn(tr: fdb.Transaction) -> None:
            meta = self._load_meta_tr(tr, metric_id)
            if meta is None:
                raise ValueError("metric not found")
            step, slots, typ = meta
            slot = self._slot_for(ts, step, slots)
            flags = self.FLAG_VALID
            window = ts // step
            tr.set(self._value_key(metric_id, slot), _pack_value_record(window, value, flags))

        self._run_transaction(txn)

    def _slot_for(self, ts: int, step: int, slots: int) -> int:
        if step <= 0:
            raise ValueError("step must be positive")
        return (ts // step) % slots

    def _segments_for(
        self, start_slot: int, count: int, slots: int
    ) -> List[Tuple[int, int]]:
        if count <= 0:
            return []
        if start_slot + count <= slots:
            return [(start_slot, start_slot + count - 1)]
        wrap = count - (slots - start_slot)
        return [
            (start_slot, slots - 1),
            (0, wrap - 1),
        ]

    def _scan_segments(
        self,
        tr: fdb.Transaction,
        metric_id: int,
        segments: Sequence[Tuple[int, int]],
        start_ts: int,
        end_ts: int,
        typ: int,
        step: int,
    ) -> List[Tuple[int, float, int]]:
        rows: List[Tuple[int, float, int]] = []
        for seg_start, seg_end in segments:
            if seg_start > seg_end:
                continue
            begin = self._value_key(metric_id, seg_start)
            end = self._value_key(metric_id, seg_end + 1)
            for kv in tr.get_range(begin, end):
                raw = kv.value
                if not raw:
                    continue
                if len(raw) < struct.calcsize(_VALUE_FMT):
                    continue
                window, val, flags = struct.unpack(_VALUE_FMT, raw)
                if not (flags & self.FLAG_VALID):
                    continue
                ts = window * step
                if ts < start_ts or ts > end_ts:
                    continue
                rows.append((ts, val, typ))
        rows.sort(key=lambda item: item[0])
        return rows

    def _meta_key(self, metric_id: int) -> bytes:
        self._ensure_u32(metric_id)
        return _PREFIX_META + metric_id.to_bytes(4, "big")

    def _meta_info_key(self, metric_id: int) -> bytes:
        self._ensure_u32(metric_id)
        return _PREFIX_META_INFO + metric_id.to_bytes(4, "big")

    def _descriptor_key(self, name: str, tags: Dict[str, str]) -> bytes:
        tag_items = tuple(sorted(tags.items()))
        return fdb.tuple.pack((5, name, tag_items))

    def _id_counter_key(self) -> bytes:
        return fdb.tuple.pack((6,))

    def _dashboard_key(self, slug: str) -> bytes:
        return fdb.tuple.pack((7, slug))

    def _dashboard_key_prefix(self) -> bytes:
        return fdb.tuple.pack((7,))

    def _value_key(self, metric_id: int, slot: int) -> bytes:
        self._ensure_u32(metric_id)
        return _PREFIX_VALUE + metric_id.to_bytes(4, "big") + slot.to_bytes(4, "big", signed=False)

    def _last_key(self, metric_id: int) -> bytes:
        return fdb.tuple.pack((3, metric_id))

    def _load_meta(self, metric_id: int) -> Optional[Tuple[int, int, int]]:
        def txn(tr: fdb.Transaction) -> Optional[Tuple[int, int, int]]:
            return self._load_meta_tr(tr, metric_id)

        return self._run_transaction(txn)

    def get_meta(self, metric_id: int) -> Optional[Tuple[int, int, int]]:
        """Public wrapper to fetch (step, slots, type) for a metric."""
        return self._load_meta(metric_id)

    def _load_meta_info(self, metric_id: int) -> Optional[Dict[str, object]]:
        def txn(tr: fdb.Transaction) -> Optional[Dict[str, object]]:
            return self._load_meta_info_tr(tr, metric_id)

        return self._run_transaction(txn)

    def _run_transaction(self, func, *args, **kwargs):
        tr = self.db.create_transaction()
        try:
            result = func(tr, *args, **kwargs)
            tr.commit().wait()
            return result
        except Exception:
            tr.cancel()
            raise

    def _load_meta_tr(
        self, tr: fdb.Transaction, metric_id: int
    ) -> Optional[Tuple[int, int, int]]:
        meta = tr.get(self._meta_key(metric_id))
        if not meta.present():
            return None
        return _unpack_meta(meta.value)

    def _load_meta_info_tr(
        self, tr: fdb.Transaction, metric_id: int
    ) -> Optional[Dict[str, object]]:
        info_bytes = tr.get(self._meta_info_key(metric_id))
        if not info_bytes.present():
            return None
        try:
            return json.loads(info_bytes.value.decode("utf-8"))
        except Exception:
            return None

    def _ensure_meta_info(
        self,
        tr: fdb.Transaction,
        metric_id: int,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        info = self._load_meta_info_tr(tr, metric_id) or {"name": "", "tags": {}}
        changed = False
        if name:
            current = info.get("name") or ""
            if current and current != name:
                raise ValueError(f"metric {metric_id} already registered with name {current}")
            if not current:
                info["name"] = name
                changed = True
        if tags:
            merged = dict(info.get("tags") or {})
            for key, val in tags.items():
                if key not in merged or merged[key] == val:
                    merged[key] = val
                elif merged[key] != val:
                    raise ValueError(
                        f"metric {metric_id} tag {key} already set to {merged[key]}"
                    )
            if merged != info.get("tags"):
                info["tags"] = merged
                changed = True
        if info and (changed or not self._load_meta_info_tr(tr, metric_id)):
            encoded = json.dumps(info, separators=(",", ":")).encode("utf-8")
            tr.set(self._meta_info_key(metric_id), encoded)

    def _ensure_descriptor(
        self,
        tr: fdb.Transaction,
        metric_id: int,
        name: Optional[str],
        tags: Optional[Dict[str, str]],
        typ: int,
        step: int,
        slots: int,
    ) -> None:
        if not name:
            return
        tags = tags or {}
        key = self._descriptor_key(name, tags)
        existing = tr.get(key)
        if existing.present():
            existing_id = struct.unpack("<q", existing.value)[0]
            if existing_id != metric_id:
                raise ValueError(f"descriptor already bound to metric {existing_id}")
            return
        tr.set(key, struct.pack("<q", metric_id))

    def _lookup_descriptor_tr(
        self, tr: fdb.Transaction, name: str, tags: Dict[str, str]
    ) -> Optional[int]:
        key = self._descriptor_key(name, tags)
        val = tr.get(key)
        if not val.present():
            return None
        return struct.unpack("<q", val.value)[0]

    def _allocate_metric_id(self, tr: fdb.Transaction) -> int:
        counter_key = self._id_counter_key()
        current = tr.get(counter_key)
        next_id = struct.unpack("<q", current.value)[0] + 1 if current.present() else 1
        tr.set(counter_key, struct.pack("<q", next_id))
        return next_id


def get_db(cluster_file: Optional[str] = None) -> fdb.Database:
    if cluster_file:
        return fdb.open(cluster_file)
    return fdb.open()


def init_tsdb(db: fdb.Database) -> FdbTsdb:
    return FdbTsdb(db, default_step=1, default_slots=3600)
