#!/usr/bin/env python3
"""
IPA ClickHouse Gold Layer Data Generator

Generates synthetic INSERT SQL for load testing the 7 IPA gold tables.
Bypasses Kafka entirely — output SQL can be loaded directly into ClickHouse.

All tables live in the patient_secure schema. Each table has 9 shared
metadata_* columns plus event-type-specific column groups. Each INSERT
covers rows of a single event type so only the relevant column group is
named; other columns get their schema DEFAULT values.

Usage:
    python generate_data.py                           # 1M rows/table, ./sql-output/
    python generate_data.py --rows 100000             # quick test
    python generate_data.py --rows 5000000 --batch-size 10000
    python generate_data.py --tables patient_audit_events activity_events
    python generate_data.py --rows-map hl7_outbound_events=8000000 patient_audit_events=3000000
"""

import argparse
import csv
import datetime
import gzip
import os
import random
import sys
import time
import uuid

# ---------------------------------------------------------------------------
# Shared fake data pools
# ---------------------------------------------------------------------------

random.seed(42)

_TENANTS   = [f"tenant_{i:04d}" for i in range(1, 51)]
_LOCATIONS = [f"loc_{i:06d}" for i in range(1, 2001)]
_USERS     = [f"user_{i:06d}" for i in range(1, 50001)]
_PATIENTS  = [str(uuid.UUID(int=random.getrandbits(128))) for _ in range(200_000)]

_IDENTIFIER_TAGS  = ["MRN", "SSN_LAST4", "DOB_HASH", "FINGERPRINT_ID", "EXTERNAL_ID"]
_ASSURANCE_LEVELS = ["LOW", "MEDIUM", "HIGH"]
_SEXES            = ["M", "F", "U"]
_SOURCES          = ["IPA", "EHR", "PORTAL", "KIOSK", "MOBILE"]
_CONFIG_ACTIONS   = ["CREATE", "UPDATE", "DELETE", "READ"]
_CONFIG_NAMES     = [
    "face_matching_threshold", "liveness_threshold", "consent_version",
    "session_timeout_minutes", "max_enrollment_attempts",
    "biometric_enabled", "photo_quality_minimum",
]
_IMPORT_RESULTS   = ["SUCCESS", "PARTIAL_SUCCESS", "FAILED", "DUPLICATE"]
_DEMOGRAPHIC_TYPES = ["FIRST_NAME", "LAST_NAME", "DATE_OF_BIRTH", "SEX", "MRN"]
_DEVICE_NAMES     = ["iPad Pro", "iPad Air", "Samsung Galaxy Tab", "Surface Pro"]
_WORKFLOWS        = ["ENROLL", "VERIFY", "IDENTIFY", "EMERGENCY", "SERVICE_DESK"]
_FIRST_NAMES      = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer"]
_LAST_NAMES       = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia"]

# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

_TS_MIN_MS = 1_704_067_200_000  # 2024-01-01T00:00:00Z in ms
_TS_MAX_MS = 1_735_689_600_000  # 2025-01-01T00:00:00Z in ms


def rand_ts() -> int:
    return random.randint(_TS_MIN_MS, _TS_MAX_MS)


def rand_ts_after(t: int) -> int:
    ceiling = min(t + 86_400_000, _TS_MAX_MS)
    return random.randint(t, max(t, ceiling))


def _ms_to_iso(ms: int) -> str:
    dt = datetime.datetime.utcfromtimestamp(ms / 1000)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _rand_date() -> str:
    return f"{random.randint(1950, 2006)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"


def _rand_uuid() -> str:
    return str(uuid.UUID(int=random.getrandbits(128)))


# ---------------------------------------------------------------------------
# SQL formatters
# ---------------------------------------------------------------------------

def ch_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _s(v):
    return "NULL" if v is None else ch_str(str(v))


def _i(v):
    return "NULL" if v is None else str(int(v))


def _f(v):
    return "NULL" if v is None else f"{v:.4f}"


def _b(v):
    return "NULL" if v is None else ("1" if v else "0")


def _arr_s(vals):
    return "[" + ", ".join(ch_str(v) for v in vals) + "]"


def _arr_id_tuples(pairs):
    if not pairs:
        return "[]"
    return "[" + ", ".join(f"({ch_str(t)}, {ch_str(v)})" for t, v in pairs) + "]"


def _arr_act_matches(matches):
    """Array(Tuple(patientId String, identifiers Array(Tuple(tag String, value String)), matchingScore Float32))"""
    if not matches:
        return "[]"
    items = []
    for pid, ids, score in matches:
        items.append(f"({ch_str(pid)}, {_arr_id_tuples(ids)}, {_f(score)})")
    return "[" + ", ".join(items) + "]"


def _arr_bio_matches(matches):
    """Array(Tuple(userId String, authenticatorId String, catalogId String, matchingScore Float32))"""
    if not matches:
        return "[]"
    return "[" + ", ".join(f"({ch_str(u)}, {ch_str(a)}, {ch_str(c)}, {_f(s)})" for u, a, c, s in matches) + "]"


def _map_ss(d):
    if not d:
        return "{}"
    items = ", ".join(f"{ch_str(k)}: {ch_str(v)}" for k, v in d.items())
    return "{" + items + "}"


# ---------------------------------------------------------------------------
# Shared metadata
# ---------------------------------------------------------------------------

_METADATA_COLS = (
    "metadata_version", "metadata_eventId", "metadata_tenantId",
    "metadata_actorUserId", "metadata_eventName", "metadata_actionType",
    "metadata_occurredAt", "metadata_workflowId", "metadata_serviceName",
)


def _metadata_vals(tenant_id, event_name, action_type, service, occurred_at):
    return [
        ch_str("1"),
        ch_str(_rand_uuid()),
        ch_str(tenant_id),
        ch_str(random.choice(_USERS)),
        ch_str(event_name),
        ch_str(action_type),
        str(occurred_at),
        ch_str(_rand_uuid()),
        ch_str(service),
    ]


def _rand_identifiers(n=None):
    n = n if n is not None else random.randint(1, 3)
    tags = random.sample(_IDENTIFIER_TAGS, min(n, len(_IDENTIFIER_TAGS)))
    return [(tag, _rand_uuid()[:8]) for tag in tags]


# ---------------------------------------------------------------------------
# face_biometric_events helpers (deeply nested, repeated sub-blocks)
# ---------------------------------------------------------------------------

_DEVICE_INFO_SUFFIXES   = ("deviceId", "deviceName", "frameRate", "height", "width")
_ANALYSIS_RESULT_SUFFIXES = (
    "quality", "qualityThreshold", "metadata",
    "livenessProbability", "livenessThreshold",
    "realnessProbability", "realnessThreshold",
)
_LIVENESS_VALIDNESS_SUFFIXES = (
    "brightness", "brightPixelPct", "darkPixelPct", "faceWidth", "faceHeight",
    "sharpness", "frontality", "livenessIsValid", "livenessFeedback",
    "acceptability", "maskProbability", "faceRollAngle",
)


def _analysis_result_cols(prefix):
    """24 columns: 7 basic + 5 deviceInfo + 12 livenessValidness."""
    cols = [f"{prefix}_{s}" for s in _ANALYSIS_RESULT_SUFFIXES]
    cols += [f"{prefix}_deviceInfo_{s}" for s in _DEVICE_INFO_SUFFIXES]
    cols += [f"{prefix}_livenessValidness_{s}" for s in _LIVENESS_VALIDNESS_SUFFIXES]
    return tuple(cols)


def _analysis_result_vals():
    quality = round(random.uniform(0.5, 1.0), 4)
    threshold = round(random.uniform(0.4, 0.8), 4)
    liveness_prob = round(random.uniform(0.0, 1.0), 4)
    liveness_thr = round(random.uniform(0.5, 0.9), 4)
    realness_prob = round(random.uniform(0.0, 1.0), 4)
    realness_thr = round(random.uniform(0.5, 0.9), 4)
    vals = [
        _f(quality), _f(threshold), _map_ss({}),
        _f(liveness_prob), _f(liveness_thr),
        _f(realness_prob), _f(realness_thr),
    ]
    # deviceInfo
    vals += [
        ch_str(_rand_uuid()[:16]),
        ch_str(random.choice(_DEVICE_NAMES)),
        str(random.choice([15, 24, 30])),
        str(random.randint(480, 1080)),
        str(random.randint(640, 1920)),
    ]
    # livenessValidness
    vals += [
        _f(round(random.uniform(0.0, 1.0), 4)),
        _f(round(random.uniform(0.0, 0.3), 4)),
        _f(round(random.uniform(0.0, 0.3), 4)),
        _f(round(random.uniform(100, 400), 2)),
        _f(round(random.uniform(100, 400), 2)),
        _f(round(random.uniform(0.0, 1.0), 4)),
        _f(round(random.uniform(-10, 10), 4)),
        _b(random.random() > 0.1),
        _arr_s([]),
        _f(round(random.uniform(0.0, 1.0), 4)),
        _f(round(random.uniform(0.0, 0.1), 4)),
        _f(round(random.uniform(-5.0, 5.0), 4)),
    ]
    return vals


# ---------------------------------------------------------------------------
# Event type weights
# ---------------------------------------------------------------------------

_PAT_AUDIT_TYPES   = ["PatientCreation", "PatientSearch", "PatientRead", "PatientDeletion", "ConsentSaved", "PatientUpdate"]
_PAT_AUDIT_WEIGHTS = [18.0, 13.1, 36.7, 26.0, 0.5, 5.8]

_ACTIVITY_TYPES   = ["PreEnrollIdentification", "Verification", "Identification", "DemographicIdentification", "PreDemographicsUpdateVerification", "EmergencySearch", "WorkflowStarted"]
_ACTIVITY_WEIGHTS = [54.0, 11.0, 15.0, 2.3, 0.04, 0.8, 16.9]

_FACE_BIO_TYPES   = ["Enrollment", "Identification", "Verification", "Deletion", "AnalysisResult"]
_FACE_BIO_WEIGHTS = [18.2, 11.7, 7.8, 19.0, 43.2]

_PHOTO_TYPES   = ["PhotoAdded", "PhotoUpdated"]
_PHOTO_WEIGHTS = [95.0, 5.0]


# ---------------------------------------------------------------------------
# patient_audit_events
# ---------------------------------------------------------------------------

_PAT_AUDIT_CREATED_COLS = _METADATA_COLS + (
    "patientCreated_patientId", "patientCreated_race", "patientCreated_dateOfBirth",
    "patientCreated_sex", "patientCreated_createdBy", "patientCreated_createdAt",
    "patientCreated_createdTimestamp", "patientCreated_identifiers",
    "patientCreated_consentId", "patientCreated_assuranceLevel",
    "patientCreated_source", "patientCreated_locationId",
)

_PAT_AUDIT_SEARCHED_COLS = _METADATA_COLS + (
    "patientSearched_patientIds", "patientSearched_searchedBy",
    "patientSearched_searchedAt", "patientSearched_createdTimestamp",
    "patientSearched_locationId",
)

_PAT_AUDIT_READ_COLS = _METADATA_COLS + (
    "patientRead_patientId", "patientRead_readBy", "patientRead_readAt",
    "patientRead_createdTimestamp", "patientRead_identifiers", "patientRead_locationId",
)

_PAT_AUDIT_DELETED_COLS = _METADATA_COLS + (
    "patientDeleted_patientId", "patientDeleted_deletedBy", "patientDeleted_deletedAt",
    "patientDeleted_createdTimestamp", "patientDeleted_identifiers", "patientDeleted_locationId",
)

_PAT_AUDIT_CONSENT_COLS = _METADATA_COLS + (
    "consentSaved_consentId", "consentSaved_firstName", "consentSaved_lastName",
    "consentSaved_email", "consentSaved_required", "consentSaved_granted",
    "consentSaved_version", "consentSaved_createdBy", "consentSaved_createdAt",
    "consentSaved_createdTimestamp", "consentSaved_locationId",
)

_PAT_AUDIT_UPDATED_COLS = _METADATA_COLS + (
    "patientUpdated_patientId", "patientUpdated_updatedDemographicType",
    "patientUpdated_oldValue", "patientUpdated_newValue", "patientUpdated_isEncrypted",
    "patientUpdated_identifiers", "patientUpdated_updatedTimestamp",
    "patientUpdated_updatedBy", "patientUpdated_locationId",
)

_PAT_AUDIT_COL_MAP = {
    "PatientCreation": _PAT_AUDIT_CREATED_COLS,
    "PatientSearch":   _PAT_AUDIT_SEARCHED_COLS,
    "PatientRead":     _PAT_AUDIT_READ_COLS,
    "PatientDeletion": _PAT_AUDIT_DELETED_COLS,
    "ConsentSaved":    _PAT_AUDIT_CONSENT_COLS,
    "PatientUpdate":   _PAT_AUDIT_UPDATED_COLS,
}


def _gen_patient_audit_row():
    event_type = random.choices(_PAT_AUDIT_TYPES, weights=_PAT_AUDIT_WEIGHTS, k=1)[0]
    tenant     = random.choice(_TENANTS)
    ts         = rand_ts()
    m          = _metadata_vals(tenant, event_type, event_type, "patient-audit-service", ts)

    if event_type == "PatientCreation":
        vals = m + [
            _s(random.choice(_PATIENTS)),
            _s(random.choice(["White", "Black", "Asian", "Other", None])),
            _s(_rand_date()),
            _s(random.choice(_SEXES)),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _arr_id_tuples(_rand_identifiers()),
            _s(_rand_uuid()),
            _s(random.choice(_ASSURANCE_LEVELS)),
            _s(random.choice(_SOURCES)),
            _s(random.choice(_LOCATIONS)),
        ]
    elif event_type == "PatientSearch":
        ids = [random.choice(_PATIENTS) for _ in range(random.randint(0, 5))]
        vals = m + [
            _arr_s(ids),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _s(random.choice(_LOCATIONS)),
        ]
    elif event_type == "PatientRead":
        vals = m + [
            _s(random.choice(_PATIENTS)),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _arr_id_tuples(_rand_identifiers()),
            _s(random.choice(_LOCATIONS)),
        ]
    elif event_type == "PatientDeletion":
        vals = m + [
            _s(random.choice(_PATIENTS)),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _arr_id_tuples(_rand_identifiers()),
            _s(random.choice(_LOCATIONS)),
        ]
    elif event_type == "ConsentSaved":
        vals = m + [
            _s(_rand_uuid()),
            _s(random.choice(_FIRST_NAMES)),
            _s(random.choice(_LAST_NAMES)),
            _s(f"{random.choice(_FIRST_NAMES).lower()}@example.com"),
            _b(True),
            _b(random.random() > 0.1),
            _s(f"v{random.randint(1, 5)}.{random.randint(0, 9)}"),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _s(random.choice(_LOCATIONS)),
        ]
    else:  # PatientUpdate
        demo_type = random.choice(_DEMOGRAPHIC_TYPES)
        vals = m + [
            _s(random.choice(_PATIENTS)),
            _s(demo_type),
            _s(_rand_uuid()[:12]),
            _s(_rand_uuid()[:12]),
            _b(random.random() > 0.5),
            _arr_id_tuples(_rand_identifiers()),
            _i(rand_ts_after(ts)),
            _s(random.choice(_USERS)),
            _s(random.choice(_LOCATIONS)),
        ]

    cols = _PAT_AUDIT_COL_MAP[event_type]
    return event_type, cols, "(" + ", ".join(vals) + ")"


# ---------------------------------------------------------------------------
# activity_events
# ---------------------------------------------------------------------------

_ACT_PRE_ENROLL_COLS = _METADATA_COLS + (
    "preEnrollIdentification_dateOfBirth",
    "preEnrollIdentification_createdBy",
    "preEnrollIdentification_createdAt",
    "preEnrollIdentification_createdTimestamp",
    "preEnrollIdentification_source",
    "preEnrollIdentification_locationId",
    "preEnrollIdentification_identifier_tag",
    "preEnrollIdentification_identifier_value",
    "preEnrollIdentification_match_patientId",
    "preEnrollIdentification_match_identifiers",
    "preEnrollIdentification_match_matchingScore",
)

_ACT_IDENTIFICATION_COLS = _METADATA_COLS + (
    "identification_matches",
    "identification_createdBy",
    "identification_createdAt",
    "identification_dateOfBirth",
    "identification_createdTimestamp",
    "identification_source",
    "identification_locationId",
)

_ACT_VERIFICATION_COLS = _METADATA_COLS + (
    "verification_patientId",
    "verification_createdBy",
    "verification_createdAt",
    "verification_createdTimestamp",
    "verification_source",
    "verification_locationId",
    "verification_identifier_tag",
    "verification_identifier_value",
    "verification_match_patientId",
    "verification_match_identifiers",
    "verification_match_matchingScore",
)

_ACT_DEMO_ID_COLS = _METADATA_COLS + (
    "demographicIdentification_demographicTypes",
    "demographicIdentification_createdBy",
    "demographicIdentification_createdAt",
    "demographicIdentification_createdTimestamp",
    "demographicIdentification_source",
    "demographicIdentification_locationId",
    "demographicIdentification_match_patientId",
    "demographicIdentification_match_identifiers",
    "demographicIdentification_match_matchingScore",
)

_ACT_PRE_DEMO_VERIFY_COLS = _METADATA_COLS + (
    "preDemographicsUpdateVerification_patientId",
    "preDemographicsUpdateVerification_createdBy",
    "preDemographicsUpdateVerification_createdAt",
    "preDemographicsUpdateVerification_createdTimestamp",
    "preDemographicsUpdateVerification_source",
    "preDemographicsUpdateVerification_identifier_tag",
    "preDemographicsUpdateVerification_identifier_value",
    "preDemographicsUpdateVerification_match_patientId",
    "preDemographicsUpdateVerification_match_identifiers",
    "preDemographicsUpdateVerification_match_matchingScore",
)

_ACT_EMERGENCY_COLS = _METADATA_COLS + (
    "emergencySearch_matches",
    "emergencySearch_createdBy",
    "emergencySearch_createdAt",
    "emergencySearch_createdTimestamp",
    "emergencySearch_source",
    "emergencySearch_locationId",
)

_ACT_WORKFLOW_COLS = _METADATA_COLS + (
    "workflowStarted_workflow",
    "workflowStarted_createdBy",
    "workflowStarted_createdAt",
    "workflowStarted_createdTimestamp",
)

_ACT_COL_MAP = {
    "PreEnrollIdentification":          _ACT_PRE_ENROLL_COLS,
    "Verification":                     _ACT_VERIFICATION_COLS,
    "Identification":                   _ACT_IDENTIFICATION_COLS,
    "DemographicIdentification":        _ACT_DEMO_ID_COLS,
    "PreDemographicsUpdateVerification": _ACT_PRE_DEMO_VERIFY_COLS,
    "EmergencySearch":                  _ACT_EMERGENCY_COLS,
    "WorkflowStarted":                  _ACT_WORKFLOW_COLS,
}


def _rand_act_matches(n_max=3):
    n = random.randint(0, n_max)
    return [(random.choice(_PATIENTS), _rand_identifiers(1), round(random.uniform(0.6, 1.0), 4)) for _ in range(n)]


def _gen_activity_row():
    event_type = random.choices(_ACTIVITY_TYPES, weights=_ACTIVITY_WEIGHTS, k=1)[0]
    tenant     = random.choice(_TENANTS)
    ts         = rand_ts()
    m          = _metadata_vals(tenant, event_type, event_type, "activity-service", ts)

    if event_type == "PreEnrollIdentification":
        id_tag, id_val = random.choice(_IDENTIFIER_TAGS), _rand_uuid()[:8]
        match_pid      = random.choice(_PATIENTS)
        vals = m + [
            _s(_rand_date()),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _s(random.choice(_SOURCES)),
            _s(random.choice(_LOCATIONS)),
            _s(id_tag),
            _s(id_val),
            _s(match_pid),
            _arr_id_tuples(_rand_identifiers(1)),
            _f(round(random.uniform(0.6, 1.0), 4)),
        ]
    elif event_type == "Identification":
        matches = _rand_act_matches()
        vals = m + [
            _arr_act_matches(matches),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _s(_rand_date()),
            _i(rand_ts_after(ts)),
            _s(random.choice(_SOURCES)),
            _s(random.choice(_LOCATIONS)),
        ]
    elif event_type == "Verification":
        id_tag, id_val = random.choice(_IDENTIFIER_TAGS), _rand_uuid()[:8]
        vals = m + [
            _s(random.choice(_PATIENTS)),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _s(random.choice(_SOURCES)),
            _s(random.choice(_LOCATIONS)),
            _s(id_tag),
            _s(id_val),
            _s(random.choice(_PATIENTS)),
            _arr_id_tuples(_rand_identifiers(1)),
            _f(round(random.uniform(0.6, 1.0), 4)),
        ]
    elif event_type == "DemographicIdentification":
        demo_types = random.sample(_DEMOGRAPHIC_TYPES, random.randint(1, 3))
        vals = m + [
            _arr_s(demo_types),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _s(random.choice(_SOURCES)),
            _s(random.choice(_LOCATIONS)),
            _s(random.choice(_PATIENTS)),
            _arr_id_tuples(_rand_identifiers(1)),
            _f(round(random.uniform(0.6, 1.0), 4)),
        ]
    elif event_type == "PreDemographicsUpdateVerification":
        id_tag, id_val = random.choice(_IDENTIFIER_TAGS), _rand_uuid()[:8]
        vals = m + [
            _s(random.choice(_PATIENTS)),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _s(random.choice(_SOURCES)),
            _s(id_tag),
            _s(id_val),
            _s(random.choice(_PATIENTS)),
            _arr_id_tuples(_rand_identifiers(1)),
            _f(round(random.uniform(0.6, 1.0), 4)),
        ]
    elif event_type == "EmergencySearch":
        matches = _rand_act_matches()
        vals = m + [
            _arr_act_matches(matches),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _s(random.choice(_SOURCES)),
            _s(random.choice(_LOCATIONS)),
        ]
    else:  # WorkflowStarted
        vals = m + [
            _s(random.choice(_WORKFLOWS)),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
        ]

    cols = _ACT_COL_MAP[event_type]
    return event_type, cols, "(" + ", ".join(vals) + ")"


# ---------------------------------------------------------------------------
# data_import_events
# ---------------------------------------------------------------------------

_DATA_IMPORT_COLS = _METADATA_COLS + (
    "dataImportProcessingResult_fhirResourceId",
    "dataImportProcessingResult_patientId",
    "dataImportProcessingResult_identifiers",
    "dataImportProcessingResult_result",
    "dataImportProcessingResult_detail",
    "dataImportProcessingResult_import_job_id",
    "dataImportProcessingResult_createdTimestamp",
)


def _gen_data_import_row():
    tenant = random.choice(_TENANTS)
    ts     = rand_ts()
    m      = _metadata_vals(tenant, "DataImportProcessingResult", "DataImportProcessingResult", "data-import-service", ts)
    result = random.choice(_IMPORT_RESULTS)
    detail = "Duplicate patient record detected" if result == "DUPLICATE" else ""
    vals   = m + [
        _s(_rand_uuid()),
        _s(random.choice(_PATIENTS)),
        _arr_id_tuples(_rand_identifiers(random.randint(1, 4))),
        _s(result),
        _s(detail),
        _s(_rand_uuid()),
        _i(rand_ts_after(ts)),
    ]
    return "DataImportProcessingResult", _DATA_IMPORT_COLS, "(" + ", ".join(vals) + ")"


# ---------------------------------------------------------------------------
# face_biometric_events
# ---------------------------------------------------------------------------

_FB_ENROLLMENT_COLS = _METADATA_COLS + (
    "enrollment_userId", "enrollment_authenticatorId", "enrollment_catalogId",
    "enrollment_createdBy", "enrollment_createdAt", "enrollment_createdTimestamp",
    "enrollment_photoId", "enrollment_source",
    "enrollment_deviceInfo_deviceId", "enrollment_deviceInfo_deviceName",
    "enrollment_deviceInfo_frameRate", "enrollment_deviceInfo_height", "enrollment_deviceInfo_width",
) + _analysis_result_cols("enrollment_analysisResult")

_FB_IDENTIFICATION_COLS = _METADATA_COLS + (
    "identification_matches",
    "identification_createdBy", "identification_createdAt", "identification_createdTimestamp",
    "identification_deviceInfo_deviceId", "identification_deviceInfo_deviceName",
    "identification_deviceInfo_frameRate", "identification_deviceInfo_height", "identification_deviceInfo_width",
) + _analysis_result_cols("identification_analysisResult")

_FB_VERIFICATION_COLS = _METADATA_COLS + (
    "verification_userId", "verification_createdBy", "verification_createdAt",
    "verification_createdTimestamp",
    "verification_match_userId", "verification_match_authenticatorId",
    "verification_match_catalogId", "verification_match_matchingScore",
    "verification_deviceInfo_deviceId", "verification_deviceInfo_deviceName",
    "verification_deviceInfo_frameRate", "verification_deviceInfo_height", "verification_deviceInfo_width",
) + _analysis_result_cols("verification_analysisResult")

_FB_DELETION_COLS = _METADATA_COLS + (
    "deletion_userId", "deletion_authenticatorId", "deletion_catalogId",
    "deletion_createdBy", "deletion_createdAt", "deletion_createdTimestamp",
)

_FB_ANALYSIS_COLS = _METADATA_COLS + (
    "analysisResult_quality", "analysisResult_qualityThreshold", "analysisResult_metadata",
    "analysisResult_livenessProbability", "analysisResult_livenessThreshold",
    "analysisResult_realnessProbability", "analysisResult_realnessThreshold",
    "analysisResult_deviceInfo_deviceId", "analysisResult_deviceInfo_deviceName",
    "analysisResult_deviceInfo_frameRate", "analysisResult_deviceInfo_height", "analysisResult_deviceInfo_width",
    "analysisResult_livenessValidness_brightness", "analysisResult_livenessValidness_brightPixelPct",
    "analysisResult_livenessValidness_darkPixelPct", "analysisResult_livenessValidness_faceWidth",
    "analysisResult_livenessValidness_faceHeight", "analysisResult_livenessValidness_sharpness",
    "analysisResult_livenessValidness_frontality", "analysisResult_livenessValidness_livenessIsValid",
    "analysisResult_livenessValidness_livenessFeedback", "analysisResult_livenessValidness_acceptability",
    "analysisResult_livenessValidness_maskProbability", "analysisResult_livenessValidness_faceRollAngle",
)

_FB_COL_MAP = {
    "Enrollment":     _FB_ENROLLMENT_COLS,
    "Identification": _FB_IDENTIFICATION_COLS,
    "Verification":   _FB_VERIFICATION_COLS,
    "Deletion":       _FB_DELETION_COLS,
    "AnalysisResult": _FB_ANALYSIS_COLS,
}


def _gen_face_biometric_row():
    event_type = random.choices(_FACE_BIO_TYPES, weights=_FACE_BIO_WEIGHTS, k=1)[0]
    tenant     = random.choice(_TENANTS)
    ts         = rand_ts()
    m          = _metadata_vals(tenant, event_type, event_type, "face-biometric-service", ts)

    if event_type == "Enrollment":
        vals = m + [
            _s(random.choice(_USERS)),
            _s(_rand_uuid()),
            _s(_rand_uuid()),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _s(_rand_uuid()),
            _s(random.choice(_SOURCES)),
            _s(_rand_uuid()[:16]),
            _s(random.choice(_DEVICE_NAMES)),
            str(random.choice([15, 24, 30])),
            str(random.randint(480, 1080)),
            str(random.randint(640, 1920)),
        ] + _analysis_result_vals()
    elif event_type == "Identification":
        n       = random.randint(0, 3)
        matches = [(_rand_uuid(), _rand_uuid(), _rand_uuid(), round(random.uniform(0.6, 1.0), 4)) for _ in range(n)]
        vals = m + [
            _arr_bio_matches(matches),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _s(_rand_uuid()[:16]),
            _s(random.choice(_DEVICE_NAMES)),
            str(random.choice([15, 24, 30])),
            str(random.randint(480, 1080)),
            str(random.randint(640, 1920)),
        ] + _analysis_result_vals()
    elif event_type == "Verification":
        vals = m + [
            _s(random.choice(_USERS)),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _s(random.choice(_USERS)),
            _s(_rand_uuid()),
            _s(_rand_uuid()),
            _f(round(random.uniform(0.6, 1.0), 4)),
            _s(_rand_uuid()[:16]),
            _s(random.choice(_DEVICE_NAMES)),
            str(random.choice([15, 24, 30])),
            str(random.randint(480, 1080)),
            str(random.randint(640, 1920)),
        ] + _analysis_result_vals()
    elif event_type == "Deletion":
        vals = m + [
            _s(random.choice(_USERS)),
            _s(_rand_uuid()),
            _s(_rand_uuid()),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
        ]
    else:  # AnalysisResult
        vals = m + _analysis_result_vals()

    cols = _FB_COL_MAP[event_type]
    return event_type, cols, "(" + ", ".join(vals) + ")"


# ---------------------------------------------------------------------------
# configuration_audit_events
# ---------------------------------------------------------------------------

_CONFIG_AUDIT_COLS = _METADATA_COLS + (
    "configurationSnapshot_name",
    "configurationSnapshot_value",
    "configurationSnapshot_action",
    "configurationSnapshot_triggeredTimestamp",
    "configurationSnapshot_triggredBy",   # intentional typo — matches DDL
)


def _gen_config_audit_row():
    tenant = random.choice(_TENANTS)
    ts     = rand_ts()
    m      = _metadata_vals(tenant, "ConfigurationSnapshot", "ConfigurationSnapshot", "configuration-audit-service", ts)
    action = random.choice(_CONFIG_ACTIONS)
    name   = random.choice(_CONFIG_NAMES)
    value  = str(round(random.uniform(0.5, 0.99), 4)) if ("threshold" in name or "minimum" in name) else str(random.randint(1, 100))
    vals   = m + [
        _s(name),
        _s(value),
        _s(action),
        _i(rand_ts_after(ts)),
        _s(random.choice(_USERS)),
    ]
    return "ConfigurationSnapshot", _CONFIG_AUDIT_COLS, "(" + ", ".join(vals) + ")"


# ---------------------------------------------------------------------------
# photo_events
# ---------------------------------------------------------------------------

_PHOTO_ADDED_COLS = _METADATA_COLS + (
    "photoAdded_patientId", "photoAdded_photoId",
    "photoAdded_createdBy", "photoAdded_createdAt", "photoAdded_createdTimestamp",
)

_PHOTO_UPDATED_COLS = _METADATA_COLS + (
    "photoUpdated_patientId", "photoUpdated_createdBy",
    "photoUpdated_createdAt", "photoUpdated_createdTimestamp",
    "photoUpdated_identifiers",
)

_PHOTO_COL_MAP = {
    "PhotoAdded":   _PHOTO_ADDED_COLS,
    "PhotoUpdated": _PHOTO_UPDATED_COLS,
}


def _gen_photo_row():
    event_type = random.choices(_PHOTO_TYPES, weights=_PHOTO_WEIGHTS, k=1)[0]
    tenant     = random.choice(_TENANTS)
    ts         = rand_ts()
    m          = _metadata_vals(tenant, event_type, event_type, "photo-service", ts)

    if event_type == "PhotoAdded":
        vals = m + [
            _s(random.choice(_PATIENTS)),
            _s(_rand_uuid()),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
        ]
    else:  # PhotoUpdated
        vals = m + [
            _s(random.choice(_PATIENTS)),
            _s(random.choice(_USERS)),
            _s(_ms_to_iso(ts)),
            _i(rand_ts_after(ts)),
            _arr_id_tuples(_rand_identifiers(random.randint(0, 2))),
        ]

    cols = _PHOTO_COL_MAP[event_type]
    return event_type, cols, "(" + ", ".join(vals) + ")"


# ---------------------------------------------------------------------------
# hl7_outbound_events
# ---------------------------------------------------------------------------

_HL7_COLS = _METADATA_COLS + (
    "photo_upload_outbound_event_id",
    "photo_upload_offset",
    "photo_upload_created_timestamp",
    "photo_upload_updated_timestamp",
    "photo_upload_errored",
    "photo_upload_hl7_sent_timestamp",
    "photo_upload_created_by",
    "photo_upload_patient_id",
    "photo_upload_processed",
)


def _gen_hl7_row():
    tenant    = random.choice(_TENANTS)
    ts        = rand_ts()
    m         = _metadata_vals(tenant, "PhotoUpload", "PhotoUpload", "hl7-outbound-service", ts)
    ts_upd    = rand_ts_after(ts)
    errored   = random.random() < 0.05
    processed = (not errored) and random.random() > 0.02
    hl7_sent  = rand_ts_after(ts_upd) if processed else 0
    vals = m + [
        _s(_rand_uuid()),
        _i(random.randrange(0, 10_000_000, 100)),
        _i(ts),
        _i(ts_upd),
        _b(errored),
        _i(hl7_sent),
        _s(random.choice(_USERS)),
        _s(random.choice(_PATIENTS)),
        _b(processed),
    ]
    return "PhotoUpload", _HL7_COLS, "(" + ", ".join(vals) + ")"


# ---------------------------------------------------------------------------
# Table registry
# ---------------------------------------------------------------------------

TABLES = {
    "patient_audit_events":       _gen_patient_audit_row,
    "activity_events":            _gen_activity_row,
    "data_import_events":         _gen_data_import_row,
    "face_biometric_events":      _gen_face_biometric_row,
    "configuration_audit_events": _gen_config_audit_row,
    "photo_events":               _gen_photo_row,
    "hl7_outbound_events":        _gen_hl7_row,
}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_generators():
    for name, gen_fn in TABLES.items():
        _event_type, cols, row = gen_fn()
        fields = _split_sql_values(row.strip("()"))
        if len(fields) != len(cols):
            raise AssertionError(
                f"{name}: generator produces {len(fields)} fields "
                f"but cols tuple has {len(cols)}"
            )


# ---------------------------------------------------------------------------
# SQL splitting (handles nested brackets for arrays/maps/tuples)
# ---------------------------------------------------------------------------

def _split_sql_values(s: str) -> list:
    parts    = []
    in_quote = False
    depth    = 0
    buf      = []
    i        = 0
    while i < len(s):
        c = s[i]
        if c == "'" and not in_quote:
            in_quote = True
            buf.append(c)
        elif c == "'" and in_quote:
            if i + 1 < len(s) and s[i + 1] == "'":
                buf.append("''")
                i += 2
                continue
            in_quote = False
            buf.append(c)
        elif not in_quote and c in "([{":
            depth += 1
            buf.append(c)
        elif not in_quote and c in ")]}":
            depth -= 1
            buf.append(c)
        elif c == "," and not in_quote and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(c)
        i += 1
    if buf:
        parts.append("".join(buf))
    return parts


# ---------------------------------------------------------------------------
# Output writer
# ---------------------------------------------------------------------------

def write_sql(table_name: str, gen_fn, rows: int, batch_size: int, out_dir: str):
    path         = os.path.join(out_dir, f"{table_name}.sql")
    written      = 0
    t0           = time.monotonic()
    report_every = max(batch_size, rows // 20)
    try:
        with open(path, "w") as f:
            while written < rows:
                chunk   = min(batch_size, rows - written)
                by_cols = {}
                for _ in range(chunk):
                    _event_type, cols, row = gen_fn()
                    by_cols.setdefault(cols, []).append(row)
                for cols, row_list in by_cols.items():
                    col_list = ", ".join(cols)
                    values   = ",\n    ".join(row_list)
                    f.write(f"INSERT INTO patient_secure.{table_name} ({col_list}) VALUES\n    {values};\n\n")
                written += chunk
                if written % report_every == 0 or written == rows:
                    elapsed = time.monotonic() - t0
                    rps     = written / elapsed if elapsed > 0 else 0
                    pct     = written / rows * 100
                    print(f"  {pct:5.1f}%  {written:>9,} / {rows:,}  ({rps:,.0f} rows/s)", flush=True)
    except OSError as e:
        try:
            os.remove(path)
        except OSError:
            pass
        raise RuntimeError(f"write_sql failed for {table_name}: {e}") from e
    return path, time.monotonic() - t0


def _sql_val_to_csv(sql_val: str) -> str:
    """Convert a single SQL-formatted value to its CSV cell representation."""
    s = sql_val.strip()
    if s == "NULL":
        return r"\N"
    if s.startswith("'") and s.endswith("'") and len(s) >= 2:
        return s[1:-1].replace("''", "'")
    return s


def write_gzipped_csv(table_name: str, gen_fn, rows: int, out_dir: str):
    """Write one gzipped CSV per event type. Returns list of (path, row_count, elapsed_sec)."""
    t0           = time.monotonic()
    written      = 0
    report_every = max(1000, rows // 20)
    # event_type -> [gz_file, csv.writer, row_count]
    handles = {}
    paths   = {}
    try:
        while written < rows:
            event_type, cols, val_str = gen_fn()
            if event_type not in handles:
                path = os.path.join(out_dir, f"{table_name}__{event_type}.csv.gz")
                paths[event_type] = path
                gz     = gzip.open(path, "wt", newline="", encoding="utf-8")
                writer = csv.writer(gz, quoting=csv.QUOTE_MINIMAL)
                writer.writerow(cols)
                handles[event_type] = [gz, writer, 0]
            entry    = handles[event_type]
            csv_vals = [_sql_val_to_csv(v) for v in _split_sql_values(val_str[1:-1])]
            entry[1].writerow(csv_vals)
            entry[2] += 1
            written  += 1
            if written % report_every == 0 or written == rows:
                elapsed = time.monotonic() - t0
                rps     = written / elapsed if elapsed > 0 else 0
                pct     = written / rows * 100
                print(f"  {pct:5.1f}%  {written:>9,} / {rows:,}  ({rps:,.0f} rows/s)", flush=True)
    except OSError as e:
        raise RuntimeError(f"write_gzipped_csv failed for {table_name}: {e}") from e
    finally:
        for entry in handles.values():
            try:
                entry[0].close()
            except OSError:
                pass

    elapsed = time.monotonic() - t0
    return [(paths[et], entry[2], elapsed) for et, entry in handles.items()]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate IPA ClickHouse load-test data")
    parser.add_argument("--rows",       type=int, default=1_000_000,
                        help="Rows per table (default: 1 000 000)")
    parser.add_argument("--batch-size", type=int, default=5_000,
                        help="INSERT batch size (default: 5 000)")
    parser.add_argument("--output-dir", type=str, default="./sql-output",
                        help="Output directory (default: ./sql-output)")
    parser.add_argument("--tables",     nargs="*", default=None,
                        help="Specific tables to generate (default: all 7)")
    parser.add_argument("--rows-map",   nargs="*", default=None, metavar="TABLE=N",
                        help="Per-table row overrides: TABLE=N ...")
    parser.add_argument("--format",     choices=["sql", "gzip-csv"], default="sql",
                        help="Output format: sql (default) or gzip-csv")
    args = parser.parse_args()

    if args.rows < 1:
        parser.error("--rows must be >= 1")
    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1")

    target_tables = args.tables if args.tables else list(TABLES.keys())
    invalid = [t for t in target_tables if t not in TABLES]
    if invalid:
        parser.error(f"Unknown tables: {invalid}. Valid: {list(TABLES.keys())}")

    rows_map = {}
    if args.rows_map:
        for entry in args.rows_map:
            if "=" not in entry:
                parser.error(f"--rows-map entries must be TABLE=N, got: {entry!r}")
            table, n = entry.split("=", 1)
            if table not in TABLES:
                parser.error(f"--rows-map unknown table {table!r}")
            try:
                rows_map[table] = int(n)
            except ValueError:
                parser.error(f"--rows-map row count must be integer, got: {n!r}")
            if rows_map[table] < 1:
                parser.error(f"--rows-map row count must be >= 1 for {table}")

    try:
        _validate_generators()
    except AssertionError as e:
        print(f"ERROR: column count mismatch — {e}", file=sys.stderr)
        raise SystemExit(1)

    try:
        os.makedirs(args.output_dir, exist_ok=True)
    except OSError as e:
        print(f"ERROR: cannot create output directory {args.output_dir!r}: {e}", file=sys.stderr)
        raise SystemExit(1)

    total_rows = sum(rows_map.get(t, args.rows) for t in target_tables)
    print(f"Generating {total_rows:,} rows across {len(target_tables)} tables → {args.output_dir}/ ({args.format})")
    print()

    total_start = time.monotonic()
    summary     = []

    for table_name in target_tables:
        gen_fn     = TABLES[table_name]
        table_rows = rows_map.get(table_name, args.rows)
        print(f"→ {table_name}  ({table_rows:,} rows)")
        try:
            if args.format == "sql":
                path, elapsed = write_sql(table_name, gen_fn, table_rows, args.batch_size, args.output_dir)
                size_mb = os.path.getsize(path) / 1_048_576
                summary.append((table_name, size_mb, elapsed))
                print(f"  → {path}  ({size_mb:.0f} MB, {elapsed:.1f}s)\n")
            else:  # gzip-csv
                results = write_gzipped_csv(table_name, gen_fn, table_rows, args.output_dir)
                total_elapsed_table = results[-1][2] if results else 0
                for path, count, _ in results:
                    size_mb    = os.path.getsize(path) / 1_048_576
                    event_type = os.path.basename(path)[len(table_name) + 2:].removesuffix(".csv.gz")
                    print(f"  → {path}  ({count:,} rows, {size_mb:.1f} MB)")
                    summary.append((f"{table_name}/{event_type}", size_mb, total_elapsed_table))
                print()
        except RuntimeError as e:
            print(f"  ERROR: {e}", flush=True)
            raise SystemExit(1)

    total_elapsed = time.monotonic() - total_start
    total_mb      = sum(s for _, s, _ in summary)

    print("─" * 62)
    print(f"  {'table/event_type':<42}  {'size':>7}  {'time':>6}")
    print("─" * 62)
    for name, mb, sec in summary:
        print(f"  {name:<42}  {mb:>6.1f}M  {sec:>5.1f}s")
    print("─" * 62)
    print(f"  {'TOTAL':<42}  {total_mb:>6.1f}M  {total_elapsed:>5.1f}s")
    print()
    print("Load into ClickHouse:")
    print(f"  python load_clickhouse.py --host <host> --password <pw> --input-dir {args.output_dir}")


if __name__ == "__main__":
    main()
