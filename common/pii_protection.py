"""PII protection for pipeline data.

Finds PII with Presidio, keeps the original values encrypted in a vault, and
returns a serving copy that holds no readable PII. Nothing is dropped.

    result = protect_dataframe(df, policy, "feature_id", client)
    result.serving   -> goes on to GeoParquet / PostGIS / metadata
    result.vault     -> rows for pii_vault.contact_pii (ciphertext only)
    result.findings  -> counts by column and entity type, never the matched text

Generic and policy-driven: any pipeline can call it with its own policy. It
does not import anything else from this project except the OpenBao client.

Fail closed: if OpenBao is unreachable or denies a call, an exception is
raised and no result is returned, so plaintext is never written.

Recovery for authorized applications (for example to send an email) goes
through reveal_contacts() with a client that holds the pii-reader role.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

import pandas as pd

from common.openbao_client import OpenBaoClient, ciphertext_key_version

SERVING_TREATMENTS = ("pseudonymize", "mask", "mask_last3", "replace_in_text", "redact")
GOVERNMENT_ID_ENTITIES = ("PH_PHILSYS", "PH_TIN", "PH_SSS", "PH_GSIS")
DEFAULT_ENTITIES = ("PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "PH_PHONE") + GOVERNMENT_ID_ENTITIES
REDACTED = "[REDACTED]"

_POLICY_KEYS = {
    "scan", "score_threshold", "entities", "columns", "skip_columns",
    "hmac_key", "enc_key", "vault_government_ids", "on_unreachable_key_service",
}
_COLUMN_KEYS = {"serving", "vault"}

# Philippine identifiers Presidio does not know by default:
# (entity, pattern name, regex, score, context words that raise the score)
_PH_RECOGNIZERS = (
    ("PH_PHONE", "ph_mobile", r"(?:\+63|0063|0)[\s-]?9\d{2}[\s-]?\d{3}[\s-]?\d{4}", 0.7, ["mobile", "cell", "phone", "contact", "call", "viber"]),
    ("PH_PHONE", "ph_metro_manila", r"(?:\(0?2\)|\b02)[\s-]?\d{4}[\s-]?\d{4}", 0.6, ["landline", "phone", "tel", "contact", "call"]),
    ("PH_PHILSYS", "philsys", r"\b\d{4}-\d{4}-\d{4}(?:-\d{4})?\b", 0.6, ["philsys", "national id", "psn", "pcn"]),
    ("PH_TIN", "tin", r"\b\d{3}-\d{3}-\d{3}(?:-\d{3,5})?\b", 0.6, ["tin", "tax"]),
    ("PH_SSS", "sss", r"\b\d{2}-\d{7}-\d\b", 0.6, ["sss"]),
    ("PH_GSIS", "gsis", r"\b\d{11}\b", 0.2, ["gsis", "bp number", "crn"]),
)


@dataclass(frozen=True)
class ColumnRule:
    serving: str
    vault: bool = True


@dataclass(frozen=True)
class PiiPolicy:
    scan: str = "all_text"
    score_threshold: float = 0.4
    entities: tuple[str, ...] = DEFAULT_ENTITIES
    columns: dict[str, ColumnRule] = field(default_factory=dict)
    skip_columns: tuple[str, ...] = ()
    hmac_key: str = "pii-hmac"
    enc_key: str = "pii-enc"
    vault_government_ids: bool = False


@dataclass(frozen=True)
class Span:
    entity: str
    start: int
    end: int
    score: float


@dataclass
class ProtectionResult:
    serving: pd.DataFrame
    vault: pd.DataFrame
    findings: list[dict[str, Any]]
    stats: dict[str, Any]


# --- policy -------------------------------------------------------------

def load_policy(config: dict | None) -> PiiPolicy:
    """Build and validate a policy from a layer's `pii:` block."""
    config = dict(config or {})
    unknown = set(config) - _POLICY_KEYS
    if unknown:
        raise ValueError(f"Unknown pii policy key(s): {sorted(unknown)}")

    scan = config.get("scan", "all_text")
    if scan not in ("all_text", "declared_only"):
        raise ValueError("pii.scan must be 'all_text' or 'declared_only'")

    threshold = float(config.get("score_threshold", 0.4))
    if not 0.0 <= threshold <= 1.0:
        raise ValueError("pii.score_threshold must be between 0 and 1")

    if config.get("on_unreachable_key_service", "fail") != "fail":
        raise ValueError("pii.on_unreachable_key_service can only be 'fail'")

    columns: dict[str, ColumnRule] = {}
    for name, rule in (config.get("columns") or {}).items():
        if not isinstance(rule, dict) or "serving" not in rule:
            raise ValueError(f"pii.columns.{name} needs a 'serving' treatment")
        extra = set(rule) - _COLUMN_KEYS
        if extra:
            raise ValueError(f"pii.columns.{name} has unknown key(s): {sorted(extra)}")
        if rule["serving"] not in SERVING_TREATMENTS:
            raise ValueError(
                f"pii.columns.{name}.serving {rule['serving']!r} is not one of {SERVING_TREATMENTS}"
            )
        columns[name] = ColumnRule(rule["serving"], bool(rule.get("vault", True)))

    return PiiPolicy(
        scan=scan,
        score_threshold=threshold,
        entities=tuple(config.get("entities") or DEFAULT_ENTITIES),
        columns=columns,
        skip_columns=tuple(config.get("skip_columns") or ()),
        hmac_key=config.get("hmac_key", "pii-hmac"),
        enc_key=config.get("enc_key", "pii-enc"),
        vault_government_ids=bool(config.get("vault_government_ids", False)),
    )


# --- detection ----------------------------------------------------------

def build_analyzer():
    """Presidio analyzer with the spaCy en_core_web_lg model and the Philippine
    recognizers. Slow to build (loads the model), so build it once per task."""
    from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer, RecognizerRegistry
    from presidio_analyzer.nlp_engine import NlpEngineProvider

    nlp_engine = NlpEngineProvider(nlp_configuration={
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_lg"}],
    }).create_engine()

    registry = RecognizerRegistry()
    registry.load_predefined_recognizers(nlp_engine=nlp_engine)
    for entity, name, regex, score, context in _PH_RECOGNIZERS:
        registry.add_recognizer(PatternRecognizer(
            supported_entity=entity,
            patterns=[Pattern(name=name, regex=regex, score=score)],
            context=context,
        ))
    return AnalyzerEngine(registry=registry, nlp_engine=nlp_engine, supported_languages=["en"])


def _is_text(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _is_text_column(series: pd.Series) -> bool:
    return pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)


def _merge_spans(spans: Iterable[Span]) -> list[Span]:
    """Merge overlapping findings (Presidio can return the same text twice,
    for example as PHONE_NUMBER and PH_PHONE). The higher-scoring entity wins."""
    merged: list[Span] = []
    for span in sorted(spans, key=lambda s: (s.start, -s.score)):
        if merged and span.start < merged[-1].end:
            last = merged[-1]
            winner = last if last.score >= span.score else span
            merged[-1] = Span(winner.entity, last.start, max(last.end, span.end), winner.score)
        else:
            merged.append(span)
    return merged


def _columns_to_scan(df: pd.DataFrame, policy: PiiPolicy, skip: set[str]) -> list[str]:
    missing = [name for name in policy.columns if name not in df.columns]
    if missing:
        raise ValueError(f"pii.columns names columns that are not in the data: {missing}")
    declared = list(policy.columns)
    if policy.scan == "declared_only":
        return declared
    text_columns = [c for c in df.columns if c not in skip and _is_text_column(df[c])]
    return list(dict.fromkeys(declared + text_columns))


def detect_pii(df: pd.DataFrame, policy: PiiPolicy, analyzer, columns: list[str]) -> dict[str, dict[int, list[Span]]]:
    """Scan the given columns. Returns {column: {row position: [Span, ...]}},
    holding only rows with findings. Repeated values are analysed once."""
    detected: dict[str, dict[int, list[Span]]] = {}
    cache: dict[str, list[Span]] = {}
    for column in columns:
        rows: dict[int, list[Span]] = {}
        for position, value in enumerate(df[column].tolist()):
            if not _is_text(value):
                continue
            spans = cache.get(value)
            if spans is None:
                results = analyzer.analyze(
                    text=value, language="en",
                    entities=list(policy.entities), score_threshold=policy.score_threshold,
                )
                spans = _merge_spans(Span(r.entity_type, r.start, r.end, r.score) for r in results)
                cache[value] = spans
            if spans:
                rows[position] = spans
        detected[column] = rows
    return detected


# --- treatments ---------------------------------------------------------

def replace_in_text(text: str, spans: Iterable[Span]) -> str:
    """Replace each span with <ENTITY_TYPE>."""
    for span in sorted(spans, key=lambda s: s.start, reverse=True):
        text = text[:span.start] + f"<{span.entity}>" + text[span.end:]
    return text


def mask_value(value: str, kind: str) -> str:
    """kind 'mask': emails keep the first character and the domain
    (j***@domain), other text keeps its first character. kind 'mask_last3':
    every digit except the last three becomes '*', other characters stay."""
    if kind == "mask":
        if "@" in value:
            local, _, domain = value.partition("@")
            return f"{local[:1]}***@{domain}"
        return f"{value[:1]}***"
    if kind == "mask_last3":
        digit_positions = [i for i, ch in enumerate(value) if ch.isdigit()]
        masked = set(digit_positions[:-3]) if len(digit_positions) > 3 else set(digit_positions)
        return "".join("*" if i in masked else ch for i, ch in enumerate(value))
    raise ValueError(f"Unknown mask kind: {kind!r}")


def _vault_text(text: str, spans: list[Span], policy: PiiPolicy) -> str:
    """The value stored in the vault. Government ID numbers are redacted from
    it unless the policy explicitly allows vaulting them."""
    if policy.vault_government_ids:
        return text
    government = [s for s in spans if s.entity in GOVERNMENT_ID_ENTITIES]
    return replace_in_text(text, government) if government else text


# --- main entry point ---------------------------------------------------

def protect_dataframe(
    df: pd.DataFrame,
    policy: PiiPolicy,
    record_id_column: str,
    client: OpenBaoClient,
    analyzer=None,
) -> ProtectionResult:
    """Return a serving copy with no readable PII, the encrypted originals, and
    a findings report. `client` must hold the loader role (HMAC and encrypt)."""
    if record_id_column not in df.columns:
        raise ValueError(f"record id column {record_id_column!r} is not in the data")
    record_ids = df[record_id_column]
    if record_ids.isna().any() or record_ids.duplicated().any():
        raise ValueError(f"{record_id_column!r} must be unique and never empty, it keys the vault")

    skip = set(policy.skip_columns) | {record_id_column}
    clash = skip & set(policy.columns)
    if clash:
        raise ValueError(f"Columns are both declared PII and skipped: {sorted(clash)}")

    columns = _columns_to_scan(df, policy, skip)
    analyzer = analyzer or build_analyzer()
    detected = detect_pii(df, policy, analyzer, columns)

    serving = df.copy()
    vault_items: list[tuple[Any, str, str]] = []   # (record id, column, text to encrypt)
    treatments: dict[str, str] = {}
    entity_counts: dict[tuple[str, str], list] = {}  # (column, entity) -> [count, min, max]
    declared_cells: dict[str, int] = {}

    for column in columns:
        rule = policy.columns.get(column)
        hits = detected[column]
        if rule is None and not hits:
            continue

        values = df[column].tolist()
        text_positions = [i for i, v in enumerate(values) if _is_text(v)]
        # Declared columns are treated in every non-empty cell. Undeclared
        # columns only where Presidio found something.
        targets = text_positions if rule is not None else sorted(hits)
        treatment = rule.serving if rule is not None else "replace_in_text"
        keep_in_vault = rule.vault if rule is not None else True
        treatments[column] = treatment

        pseudonyms: dict[str, str] = {}
        if treatment == "pseudonymize":
            unique = list(dict.fromkeys(values[i] for i in targets))
            pseudonyms = dict(zip(unique, client.hmac_batch(policy.hmac_key, unique)))

        new_values = list(values)
        for position in targets:
            text = values[position]
            spans = hits.get(position, [])
            if treatment == "pseudonymize":
                new_values[position] = pseudonyms[text]
            elif treatment in ("mask", "mask_last3"):
                new_values[position] = mask_value(text, treatment)
            elif treatment == "redact":
                new_values[position] = REDACTED
            else:
                new_values[position] = replace_in_text(text, spans)
            if keep_in_vault:
                vault_items.append((record_ids.iloc[position], column, _vault_text(text, spans, policy)))

        serving[column] = pd.Series(new_values, index=serving.index, dtype=object)

        for spans in hits.values():
            for span in spans:
                entry = entity_counts.setdefault((column, span.entity), [0, 1.0, 0.0])
                entry[0] += 1
                entry[1] = min(entry[1], span.score)
                entry[2] = max(entry[2], span.score)
        if rule is not None:
            declared_cells[column] = len(text_positions)

    ciphertexts = client.encrypt_batch(policy.enc_key, [text for _, _, text in vault_items])
    vault = pd.DataFrame({
        "record_id": [item[0] for item in vault_items],
        "column_name": [item[1] for item in vault_items],
        "ciphertext": ciphertexts,
        "key_version": [ciphertext_key_version(c) for c in ciphertexts],
    })

    findings: list[dict[str, Any]] = []
    for (column, entity), (count, low, high) in sorted(entity_counts.items()):
        findings.append({
            "column": column, "entity": entity, "count": count,
            "min_score": round(low, 3), "max_score": round(high, 3),
            "treatment": treatments[column], "declared": column in policy.columns,
        })
    for column, cells in declared_cells.items():
        findings.append({
            "column": column, "entity": "declared", "count": cells,
            "min_score": None, "max_score": None,
            "treatment": treatments[column], "declared": True,
        })

    stats = {
        "rows": len(df),
        "columns_scanned": columns,
        "columns_protected": sorted(treatments),
        "vault_rows": len(vault),
    }
    return ProtectionResult(serving=serving, vault=vault, findings=findings, stats=stats)


def reveal_contacts(
    client: OpenBaoClient,
    vault_rows: pd.DataFrame,
    enc_key: str = "pii-enc",
    record_ids: Iterable[Any] | None = None,
    columns: Iterable[str] | None = None,
) -> dict[Any, dict[str, str]]:
    """Decrypt vault rows into {record id: {column: original value}}.

    For authorized applications only. `client` must hold the pii-reader role.
    Every call is written to the OpenBao audit log. Ask for the narrowest
    record ids and columns the use needs."""
    rows = vault_rows
    if record_ids is not None:
        rows = rows[rows["record_id"].isin(list(record_ids))]
    if columns is not None:
        rows = rows[rows["column_name"].isin(list(columns))]
    plaintexts = client.decrypt_batch(enc_key, rows["ciphertext"].tolist())
    revealed: dict[Any, dict[str, str]] = {}
    for record_id, column, text in zip(rows["record_id"], rows["column_name"], plaintexts):
        revealed.setdefault(record_id, {})[column] = text  # type: ignore[assignment]
    return revealed
