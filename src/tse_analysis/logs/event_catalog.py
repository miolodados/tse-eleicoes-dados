"""Catálogo declarativo de eventos de log da urna (allowlist semântica com zero PII)."""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from tse_analysis.config import ExecutionConfig

UNKNOWN_EVENT_CODE: str = "UNKNOWN_EVENT"
MALFORMED_LINE_CODE: str = "MALFORMED_LINE"

ALLOWED_EXTRACTOR_FIELDS: tuple[str, ...] = ("biometric_attempt_num",)

FORBIDDEN_EXTRACTOR_TOKENS: tuple[str, ...] = (
    "cpf",
    "titulo",
    "eleitor",
    "mesario",
    "message",
    "text",
    "mac",
    "voto",
    "votavel",
    "candidato",
)

REQUIRED_EVENT_CODES: tuple[str, ...] = (
    "LOGD_STARTED",
    "SECTION_OPENED",
    "VOTER_ENABLED",
    "BIOMETRIC_READ_ATTEMPT",
    "BIOMETRIC_RECOGNIZED",
    "BIOMETRIC_NOT_RECOGNIZED",
    "NO_BIOMETRIC_REGISTERED",
    "MANUAL_ENABLE_PATH",
    "INACTIVITY_TIMEOUT_45S",
    "INVALID_KEY_PRESSED",
    "CARGO_VOTE_RECORDED",
    "VOTER_VOTE_COMPUTED",
    "VOTING_SUSPENDED",
    "VOTING_RESUMED",
    "CONTINGENCY_EVENT",
    "SECTION_CLOSED",
)


class EventCatalogValidationError(ValueError):
    """Erro lançado quando configs/event_catalog.yml viola o contrato de allowlist e zero-PII."""


@dataclass(frozen=True, slots=True)
class ClassificationResult:
    """Resultado sanitizado da classificação semântica de uma linha de log (sem texto livre)."""

    event_code: str
    biometric_attempt_num: int | None = None
    pattern_id: str | None = None


@dataclass(frozen=True, slots=True)
class EventPattern:
    """Regra compilada de classificação semântica da allowlist de eventos."""

    pattern_id: str
    event_code: str
    applications: tuple[str, ...]
    regex: str
    compiled_regex: re.Pattern[str]
    extractors: dict[str, str] = field(default_factory=dict)
    description: str = ""

    def matches_application(self, application: str) -> bool:
        """Verifica se a regra se aplica ao aplicativo emissor do log."""
        if "*" in self.applications:
            return True
        normalized_app = application.strip().upper()
        return any(app.upper() == normalized_app for app in self.applications)


@dataclass(frozen=True, slots=True)
class TurnBoundaryPatterns:
    """Padrões regex (compatíveis com Rust/RE2 e Python re) para fronteira de 1º/2º turno."""

    turn1_init: str
    turn2_init: str
    boot_header: str
    turn1_close: str
    compiled_turn1_init: re.Pattern[str]
    compiled_turn2_init: re.Pattern[str]
    compiled_boot_header: re.Pattern[str]
    compiled_turn1_close: re.Pattern[str]


_DEFAULT_TURN_BOUNDARY_RAW: dict[str, str] = {
    "turn1_init": (
        r"(?i)(?:iniciando\s+aplica[cç][aã]o\s*[-–:]\s*1[º°o]?\s*turno"
        r"|turno(?:\s+da\s+ue)?\s*[:=\-]\s*\[?1\]?(?:\D|$)"
        r"|\b1[º°o]\s*turno\b|\bprimeiro\s+turno\b"
        r"|\bpleito\s*[:=\-]?\s*\[?0?3220\]?(?:\D|$)|\bo03220[a-z]{2})"
    ),
    "turn2_init": (
        r"(?i)(?:iniciando\s+aplica[cç][aã]o\s*[-–:]\s*2[º°o]?\s*turno"
        r"|turno(?:\s+da\s+ue)?\s*[:=\-]\s*\[?2\]?(?:\D|$)"
        r"|\b2[º°o]\s*turno\b|\bsegundo\s+turno\b"
        r"|\bpleito\s*[:=\-]?\s*\[?0?3221\]?(?:\D|$)|\bo03221[a-z]{2})"
    ),
    "boot_header": r"(?i)(?:in[ií]cio\s+das\s+opera[cç][oõ]es\s+do\s+logd|logd\s+iniciado)",
    "turn1_close": (
        r"(?i)(?:encerramento\s+da\s+(?:vota[cç][aã]o|se[cç][aã]o)"
        r"|vota[cç][aã]o\s+encerrada|se[cç][aã]o\s+encerrada"
        r"|emiss[aã]o\s+do\s+boletim\s+de\s+urna"
        r"|gerando\s+arquivo\s+de\s+resultado\s*\[(?:imgbu\.dat|log\.jez)\])"
    ),
}


def _validate_and_build_turn_boundary_patterns(raw: Any) -> TurnBoundaryPatterns:
    if raw is None:
        raw_map = dict(_DEFAULT_TURN_BOUNDARY_RAW)
    elif isinstance(raw, dict):
        raw_map = {**_DEFAULT_TURN_BOUNDARY_RAW, **{str(k): str(v) for k, v in raw.items()}}
    else:
        raise EventCatalogValidationError(
            "Campo 'turn_boundary_patterns' no catálogo deve ser um mapeamento YAML."
        )

    compiled: dict[str, re.Pattern[str]] = {}
    patterns: dict[str, str] = {}
    for key in ("turn1_init", "turn2_init", "boot_header", "turn1_close"):
        regex_str = str(raw_map.get(key, "")).strip()
        if not regex_str:
            raise EventCatalogValidationError(
                f"Padrão de fronteira '{key}' em 'turn_boundary_patterns' não pode ser vazio."
            )
        try:
            pat = re.compile(regex_str)
        except re.error as exc:
            raise EventCatalogValidationError(
                f"Expressão regular inválida em 'turn_boundary_patterns.{key}': {exc}"
            ) from exc
        for group_name in pat.groupindex:
            lowered_group = group_name.lower()
            if any(token in lowered_group for token in FORBIDDEN_EXTRACTOR_TOKENS):
                raise EventCatalogValidationError(
                    f"Padrão 'turn_boundary_patterns.{key}' declara grupo proibido "
                    f"'{group_name}' (violação de política zero-PII/zero-texto-livre)."
                )
        patterns[key] = regex_str
        compiled[key] = pat

    return TurnBoundaryPatterns(
        turn1_init=patterns["turn1_init"],
        turn2_init=patterns["turn2_init"],
        boot_header=patterns["boot_header"],
        turn1_close=patterns["turn1_close"],
        compiled_turn1_init=compiled["turn1_init"],
        compiled_turn2_init=compiled["turn2_init"],
        compiled_boot_header=compiled["boot_header"],
        compiled_turn1_close=compiled["turn1_close"],
    )


def default_turn_boundary_patterns() -> TurnBoundaryPatterns:
    """Retorna os padrões padrão compilados de fronteira de turno."""
    return _validate_and_build_turn_boundary_patterns(None)


@dataclass(frozen=True, slots=True)
class EventCatalog:
    """Catálogo declarativo compilado para classificação de eventos do Ecossistema da Urna."""

    schema_version: str
    fallback_event_code: str
    malformed_event_code: str
    patterns: tuple[EventPattern, ...]
    turn_boundary_patterns: TurnBoundaryPatterns = field(
        default_factory=default_turn_boundary_patterns
    )

    @property
    def event_codes(self) -> frozenset[str]:
        """Retorna o conjunto de todos os event_codes declarados nas regras do catálogo."""
        return frozenset(p.event_code for p in self.patterns)

    def classify(
        self,
        message_text: str,
        application: str = "VOTA",
    ) -> ClassificationResult:
        """Classifica a mensagem em memória efêmera e descarta qualquer texto livre."""
        normalized_msg = message_text.strip()
        if not normalized_msg:
            return ClassificationResult(
                event_code=self.fallback_event_code,
                biometric_attempt_num=None,
                pattern_id=None,
            )

        for pattern in self.patterns:
            if not pattern.matches_application(application):
                continue

            match = pattern.compiled_regex.search(normalized_msg)
            if match is None:
                continue

            attempt_num: int | None = None
            group_name = pattern.extractors.get("biometric_attempt_num")
            if group_name:
                raw_val = match.groupdict().get(group_name)
                if raw_val is not None:
                    try:
                        parsed_int = int(raw_val)
                        if 1 <= parsed_int <= 127:
                            attempt_num = parsed_int
                    except ValueError:
                        attempt_num = None

            return ClassificationResult(
                event_code=pattern.event_code,
                biometric_attempt_num=attempt_num,
                pattern_id=pattern.pattern_id,
            )

        return ClassificationResult(
            event_code=self.fallback_event_code,
            biometric_attempt_num=None,
            pattern_id=None,
        )


def _validate_and_build_pattern(raw: Any, seen_pattern_ids: set[str]) -> EventPattern:
    if not isinstance(raw, dict):
        raise EventCatalogValidationError("Cada item em 'patterns' deve ser um objeto YAML.")

    for req_field in ("pattern_id", "event_code", "applications", "regex"):
        if req_field not in raw or raw[req_field] is None:
            raise EventCatalogValidationError(
                f"Regra de evento inválida: campo obrigatório ausente '{req_field}'."
            )

    pattern_id = str(raw["pattern_id"]).strip()
    if not pattern_id:
        raise EventCatalogValidationError("Campo 'pattern_id' não pode ser vazio.")
    if pattern_id in seen_pattern_ids:
        raise EventCatalogValidationError(f"pattern_id duplicado no catálogo: '{pattern_id}'.")
    seen_pattern_ids.add(pattern_id)

    event_code = str(raw["event_code"]).strip()
    if not event_code or not re.fullmatch(r"[A-Z][A-Z0-9_]*", event_code):
        raise EventCatalogValidationError(
            f"Regra '{pattern_id}' possui event_code inválido '{event_code}'. "
            "Use UPPER_SNAKE_CASE."
        )

    apps_raw = raw["applications"]
    if not isinstance(apps_raw, list) or not apps_raw:
        raise EventCatalogValidationError(
            f"Regra '{pattern_id}' deve definir 'applications' como lista não-vazia."
        )
    applications = tuple(str(app).strip() for app in apps_raw if str(app).strip())

    regex_str = str(raw["regex"])
    if not regex_str.strip():
        raise EventCatalogValidationError(f"Regra '{pattern_id}' possui regex vazia.")
    try:
        compiled = re.compile(regex_str)
    except re.error as exc:
        raise EventCatalogValidationError(
            f"Expressão regular inválida na regra '{pattern_id}': {exc}"
        ) from exc

    # Validação de grupos nomeados e extratores para impedir captura de PII ou texto livre
    for group_name in compiled.groupindex:
        lowered_group = group_name.lower()
        if any(token in lowered_group for token in FORBIDDEN_EXTRACTOR_TOKENS):
            raise EventCatalogValidationError(
                f"Regra '{pattern_id}' declara grupo regex proibido '{group_name}' "
                "(violação de política zero-PII/zero-texto-livre)."
            )

    extractors_raw = raw.get("extractors") or {}
    if not isinstance(extractors_raw, dict):
        raise EventCatalogValidationError(
            f"Campo 'extractors' na regra '{pattern_id}' deve ser um mapeamento."
        )

    extractors: dict[str, str] = {}
    for target_field, source_group in extractors_raw.items():
        field_str = str(target_field).strip()
        group_str = str(source_group).strip()
        lowered_field = field_str.lower()
        if (
            any(token in lowered_field for token in FORBIDDEN_EXTRACTOR_TOKENS)
            or field_str not in ALLOWED_EXTRACTOR_FIELDS
        ):
            raise EventCatalogValidationError(
                f"Regra '{pattern_id}' tenta extrair campo não permitido '{field_str}'. "
                f"Campos permitidos: {ALLOWED_EXTRACTOR_FIELDS}."
            )
        if group_str not in compiled.groupindex:
            raise EventCatalogValidationError(
                f"Regra '{pattern_id}' referencia grupo regex inexistente '{group_str}' "
                f"para o extrator '{field_str}'."
            )
        extractors[field_str] = group_str

    return EventPattern(
        pattern_id=pattern_id,
        event_code=event_code,
        applications=applications,
        regex=regex_str,
        compiled_regex=compiled,
        extractors=extractors,
        description=str(raw.get("description", "")).strip(),
    )


def load_event_catalog_from_payload(
    payload: Any,
    *,
    require_all_standard_codes: bool = True,
) -> EventCatalog:
    """Compila e valida o catálogo a partir de um dicionário YAML já carregado."""
    if not isinstance(payload, dict):
        raise EventCatalogValidationError(
            "O conteúdo de event_catalog.yml deve ser um objeto YAML raiz."
        )

    raw_patterns = payload.get("patterns")
    if not isinstance(raw_patterns, list) or not raw_patterns:
        raise EventCatalogValidationError(
            "O catálogo de eventos deve conter uma lista não-vazia em 'patterns'."
        )

    seen_ids: set[str] = set()
    patterns = tuple(_validate_and_build_pattern(item, seen_ids) for item in raw_patterns)
    turn_boundary_patterns = _validate_and_build_turn_boundary_patterns(
        payload.get("turn_boundary_patterns")
    )

    catalog = EventCatalog(
        schema_version=str(payload.get("schema_version", "1.0")),
        fallback_event_code=str(payload.get("fallback_event_code", UNKNOWN_EVENT_CODE)),
        malformed_event_code=str(payload.get("malformed_event_code", MALFORMED_LINE_CODE)),
        patterns=patterns,
        turn_boundary_patterns=turn_boundary_patterns,
    )

    if require_all_standard_codes:
        present_codes = catalog.event_codes
        missing = [code for code in REQUIRED_EVENT_CODES if code not in present_codes]
        if missing:
            raise EventCatalogValidationError(
                f"Catálogo de eventos incompleto. Códigos obrigatórios ausentes: {missing}."
            )

    return catalog


def load_event_catalog_from_string(
    yaml_str: str,
    *,
    require_all_standard_codes: bool = True,
) -> EventCatalog:
    """Carrega, compila e valida o catálogo a partir de uma string YAML."""
    payload = yaml.safe_load(yaml_str)
    return load_event_catalog_from_payload(
        payload,
        require_all_standard_codes=require_all_standard_codes,
    )


def load_event_catalog(
    catalog_path: str | Path | None = None,
    *,
    require_all_standard_codes: bool = True,
) -> EventCatalog:
    """Carrega, compila e valida o catálogo declarativo configs/event_catalog.yml."""
    if catalog_path is None:
        resolved_path = ExecutionConfig().config_dir / "event_catalog.yml"
    else:
        resolved_path = Path(catalog_path)

    if not resolved_path.exists():
        raise FileNotFoundError(f"Arquivo de catálogo de eventos não encontrado: {resolved_path}")

    with resolved_path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh)

    return load_event_catalog_from_payload(
        payload,
        require_all_standard_codes=require_all_standard_codes,
    )


@functools.lru_cache(maxsize=1)
def _get_default_event_catalog() -> EventCatalog:
    return load_event_catalog()


def classify_log_message(
    message_text: str,
    application: str = "VOTA",
    *,
    catalog: EventCatalog | None = None,
) -> ClassificationResult:
    """Classifica uma mensagem de log contra a allowlist de configs/event_catalog.yml.

    Qualquer mensagem fora da allowlist recebe event_code = 'UNKNOWN_EVENT' e
    biometric_attempt_num = None, sem reter texto livre ou PII.
    """
    active_catalog = catalog if catalog is not None else _get_default_event_catalog()
    return active_catalog.classify(message_text=message_text, application=application)
