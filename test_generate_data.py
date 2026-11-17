#!/usr/bin/env python3
"""
Tests for generate_data.py — IPA ClickHouse Gold Layer Data Generator.
"""

import os
import tempfile
import unittest

from generate_data import (
    _METADATA_COLS,
    _PAT_AUDIT_TYPES, _PAT_AUDIT_WEIGHTS,
    _ACTIVITY_TYPES, _ACTIVITY_WEIGHTS,
    _FACE_BIO_TYPES, _FACE_BIO_WEIGHTS,
    _PHOTO_TYPES, _PHOTO_WEIGHTS,
    _TS_MIN_MS, _TS_MAX_MS,
    TABLES,
    _split_sql_values,
    _sql_val_to_csv,
    _validate_generators,
    ch_str,
    rand_ts,
    rand_ts_after,
    write_sql,
    write_gzipped_csv,
    _gen_patient_audit_row,
    _gen_activity_row,
    _gen_data_import_row,
    _gen_face_biometric_row,
    _gen_config_audit_row,
    _gen_photo_row,
    _gen_hl7_row,
    _arr_id_tuples,
    _arr_act_matches,
    _arr_bio_matches,
    _map_ss,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check_row(test, name, gen_fn):
    result = gen_fn()
    test.assertIsInstance(result, tuple, f"{name}: result should be a 3-tuple")
    test.assertEqual(len(result), 3, f"{name}: result should be (event_type, cols, values_str)")
    event_type, cols, values_str = result
    test.assertIsInstance(event_type, str, f"{name}: event_type should be a str")
    test.assertIsInstance(cols, tuple, f"{name}: cols should be a tuple")
    test.assertTrue(values_str.startswith("("), f"{name}: values_str should start with (")
    test.assertTrue(values_str.endswith(")"), f"{name}: values_str should end with )")
    fields = _split_sql_values(values_str[1:-1])
    test.assertEqual(len(fields), len(cols),
                     f"{name}: {len(fields)} fields but {len(cols)} cols")
    return event_type, cols, values_str, fields


# ---------------------------------------------------------------------------
# ch_str
# ---------------------------------------------------------------------------

class TestChStr(unittest.TestCase):

    def test_plain(self):
        self.assertEqual(ch_str("hello"), "'hello'")

    def test_empty(self):
        self.assertEqual(ch_str(""), "''")

    def test_escapes_single_quote(self):
        self.assertEqual(ch_str("it's"), "'it''s'")

    def test_multiple_quotes(self):
        self.assertEqual(ch_str("a'b'c"), "'a''b''c'")


# ---------------------------------------------------------------------------
# _split_sql_values
# ---------------------------------------------------------------------------

class TestSplitSqlValues(unittest.TestCase):

    def test_simple(self):
        self.assertEqual(_split_sql_values("'a','b','c'"), ["'a'", "'b'", "'c'"])

    def test_quoted_comma(self):
        self.assertEqual(_split_sql_values("'a,b','c'"), ["'a,b'", "'c'"])

    def test_escaped_quote(self):
        self.assertEqual(_split_sql_values("'it''s',1"), ["'it''s'", "1"])

    def test_array_not_split(self):
        result = _split_sql_values("'x',[('MRN','abc')],1")
        self.assertEqual(len(result), 3)
        self.assertIn("[('MRN','abc')]", result[1])

    def test_nested_array(self):
        inner = "[('pid',[('MRN','123')],0.9500)]"
        result = _split_sql_values(f"'a',{inner},'b'")
        self.assertEqual(len(result), 3)

    def test_map_not_split(self):
        result = _split_sql_values("{'k': 'v'},1")
        self.assertEqual(len(result), 2)
        self.assertIn("{", result[0])

    def test_null(self):
        self.assertEqual(_split_sql_values("NULL,NULL"), ["NULL", "NULL"])

    def test_integer(self):
        self.assertEqual(_split_sql_values("123,456"), ["123", "456"])


# ---------------------------------------------------------------------------
# rand_ts / rand_ts_after
# ---------------------------------------------------------------------------

class TestTimestamps(unittest.TestCase):

    def test_rand_ts_is_int(self):
        t = rand_ts()
        self.assertIsInstance(t, int)

    def test_rand_ts_in_range(self):
        for _ in range(50):
            t = rand_ts()
            self.assertGreaterEqual(t, _TS_MIN_MS)
            self.assertLessEqual(t, _TS_MAX_MS)

    def test_rand_ts_after_gte(self):
        for _ in range(50):
            t1 = rand_ts()
            t2 = rand_ts_after(t1)
            self.assertGreaterEqual(t2, t1)

    def test_rand_ts_after_is_int(self):
        t = rand_ts_after(rand_ts())
        self.assertIsInstance(t, int)

    def test_rand_ts_after_at_max(self):
        t2 = rand_ts_after(_TS_MAX_MS)
        self.assertEqual(t2, _TS_MAX_MS)


# ---------------------------------------------------------------------------
# Array / map formatters
# ---------------------------------------------------------------------------

class TestFormatters(unittest.TestCase):

    def test_arr_id_tuples_empty(self):
        self.assertEqual(_arr_id_tuples([]), "[]")

    def test_arr_id_tuples_one(self):
        result = _arr_id_tuples([("MRN", "123")])
        self.assertEqual(result, "[('MRN', '123')]")

    def test_arr_id_tuples_escaping(self):
        result = _arr_id_tuples([("O'Brien", "val")])
        self.assertIn("''", result)

    def test_arr_act_matches_empty(self):
        self.assertEqual(_arr_act_matches([]), "[]")

    def test_arr_act_matches_one(self):
        result = _arr_act_matches([("pid1", [("MRN", "abc")], 0.95)])
        self.assertTrue(result.startswith("["))
        self.assertIn("0.9500", result)
        self.assertIn("MRN", result)

    def test_arr_bio_matches_empty(self):
        self.assertEqual(_arr_bio_matches([]), "[]")

    def test_arr_bio_matches_one(self):
        result = _arr_bio_matches([("u1", "a1", "c1", 0.9)])
        self.assertIn("0.9000", result)

    def test_map_ss_empty(self):
        self.assertEqual(_map_ss({}), "{}")

    def test_map_ss_one_pair(self):
        result = _map_ss({"key": "val"})
        self.assertTrue(result.startswith("{"))
        self.assertIn("'key'", result)
        self.assertIn("'val'", result)


# ---------------------------------------------------------------------------
# Generator rows
# ---------------------------------------------------------------------------

class TestPatientAuditRow(unittest.TestCase):

    def test_returns_correct_structure(self):
        _check_row(self, "patient_audit_events", _gen_patient_audit_row)

    def test_starts_with_metadata_cols(self):
        _, cols, _, _ = _check_row(self, "patient_audit_events", _gen_patient_audit_row)
        self.assertEqual(cols[:9], _METADATA_COLS)

    def test_multiple_rows_stable(self):
        for _ in range(20):
            _check_row(self, "patient_audit_events", _gen_patient_audit_row)

    def test_event_type_in_known_types(self):
        for _ in range(30):
            event_type, cols, values_str, fields = _check_row(self, "patient_audit_events", _gen_patient_audit_row)
            metadata_eventName_idx = list(cols).index("metadata_eventName")
            raw = fields[metadata_eventName_idx].strip()
            event_type = raw.strip("'")
            self.assertIn(event_type, _PAT_AUDIT_TYPES)


class TestActivityRow(unittest.TestCase):

    def test_returns_correct_structure(self):
        _check_row(self, "activity_events", _gen_activity_row)

    def test_starts_with_metadata_cols(self):
        _, cols, _, _ = _check_row(self, "activity_events", _gen_activity_row)
        self.assertEqual(cols[:9], _METADATA_COLS)

    def test_multiple_rows_stable(self):
        for _ in range(20):
            _check_row(self, "activity_events", _gen_activity_row)

    def test_event_type_in_known_types(self):
        for _ in range(30):
            event_type, cols, values_str, fields = _check_row(self, "activity_events", _gen_activity_row)
            idx = list(cols).index("metadata_eventName")
            event_type = fields[idx].strip().strip("'")
            self.assertIn(event_type, _ACTIVITY_TYPES)


class TestDataImportRow(unittest.TestCase):

    def test_returns_correct_structure(self):
        _check_row(self, "data_import_events", _gen_data_import_row)

    def test_multiple_rows_stable(self):
        for _ in range(20):
            _check_row(self, "data_import_events", _gen_data_import_row)

    def test_has_processing_result_cols(self):
        _, cols, _, _ = _check_row(self, "data_import_events", _gen_data_import_row)
        self.assertIn("dataImportProcessingResult_result", cols)
        self.assertIn("dataImportProcessingResult_import_job_id", cols)


class TestFaceBiometricRow(unittest.TestCase):

    def test_returns_correct_structure(self):
        _check_row(self, "face_biometric_events", _gen_face_biometric_row)

    def test_starts_with_metadata_cols(self):
        _, cols, _, _ = _check_row(self, "face_biometric_events", _gen_face_biometric_row)
        self.assertEqual(cols[:9], _METADATA_COLS)

    def test_multiple_rows_stable(self):
        for _ in range(20):
            _check_row(self, "face_biometric_events", _gen_face_biometric_row)

    def test_event_type_in_known_types(self):
        for _ in range(30):
            event_type, cols, values_str, fields = _check_row(self, "face_biometric_events", _gen_face_biometric_row)
            idx = list(cols).index("metadata_eventName")
            event_type = fields[idx].strip().strip("'")
            self.assertIn(event_type, _FACE_BIO_TYPES)


class TestConfigAuditRow(unittest.TestCase):

    def test_returns_correct_structure(self):
        _check_row(self, "configuration_audit_events", _gen_config_audit_row)

    def test_has_typo_column(self):
        _, cols, _, _ = _check_row(self, "configuration_audit_events", _gen_config_audit_row)
        self.assertIn("configurationSnapshot_triggredBy", cols)
        self.assertNotIn("configurationSnapshot_triggeredBy", cols)

    def test_multiple_rows_stable(self):
        for _ in range(20):
            _check_row(self, "configuration_audit_events", _gen_config_audit_row)


class TestPhotoRow(unittest.TestCase):

    def test_returns_correct_structure(self):
        _check_row(self, "photo_events", _gen_photo_row)

    def test_starts_with_metadata_cols(self):
        _, cols, _, _ = _check_row(self, "photo_events", _gen_photo_row)
        self.assertEqual(cols[:9], _METADATA_COLS)

    def test_event_type_in_known_types(self):
        for _ in range(30):
            event_type, cols, values_str, fields = _check_row(self, "photo_events", _gen_photo_row)
            idx = list(cols).index("metadata_eventName")
            event_type = fields[idx].strip().strip("'")
            self.assertIn(event_type, _PHOTO_TYPES)

    def test_multiple_rows_stable(self):
        for _ in range(20):
            _check_row(self, "photo_events", _gen_photo_row)


class TestHl7Row(unittest.TestCase):

    def test_returns_correct_structure(self):
        _check_row(self, "hl7_outbound_events", _gen_hl7_row)

    def test_has_photo_upload_cols(self):
        _, cols, _, _ = _check_row(self, "hl7_outbound_events", _gen_hl7_row)
        self.assertIn("photo_upload_outbound_event_id", cols)
        self.assertIn("photo_upload_processed", cols)

    def test_multiple_rows_stable(self):
        for _ in range(20):
            _check_row(self, "hl7_outbound_events", _gen_hl7_row)


# ---------------------------------------------------------------------------
# _validate_generators
# ---------------------------------------------------------------------------

class TestValidateGenerators(unittest.TestCase):

    def test_all_valid(self):
        _validate_generators()  # should not raise


# ---------------------------------------------------------------------------
# write_sql
# ---------------------------------------------------------------------------

class TestWriteSql(unittest.TestCase):

    def _write(self, table_name, rows=50):
        gen_fn = TABLES[table_name]
        with tempfile.TemporaryDirectory() as tmpdir:
            path, elapsed = write_sql(table_name, gen_fn, rows, 10, tmpdir)
            self.assertTrue(os.path.exists(path))
            with open(path) as f:
                content = f.read()
        return content, elapsed

    def test_creates_file(self):
        gen_fn = TABLES["configuration_audit_events"]
        with tempfile.TemporaryDirectory() as tmpdir:
            path, _ = write_sql("configuration_audit_events", gen_fn, 10, 5, tmpdir)
            self.assertTrue(os.path.exists(path))

    def test_insert_uses_patient_secure_schema(self):
        content, _ = self._write("configuration_audit_events", rows=10)
        self.assertIn("INSERT INTO patient_secure.configuration_audit_events", content)

    def test_insert_has_col_list(self):
        content, _ = self._write("photo_events", rows=20)
        self.assertIn("metadata_eventId", content)

    def test_insert_has_values(self):
        content, _ = self._write("data_import_events", rows=10)
        self.assertIn("VALUES", content)

    def test_elapsed_is_positive(self):
        _, elapsed = self._write("hl7_outbound_events", rows=10)
        self.assertGreater(elapsed, 0)

    def test_unwritable_output_directory(self):
        gen_fn = TABLES["photo_events"]
        with self.assertRaises((RuntimeError, SystemExit, OSError)):
            write_sql("photo_events", gen_fn, 10, 5, "/nonexistent_dir/subdir")


# ---------------------------------------------------------------------------
# _sql_val_to_csv
# ---------------------------------------------------------------------------

class TestSqlValToCsv(unittest.TestCase):

    def test_null_becomes_backslash_n(self):
        self.assertEqual(_sql_val_to_csv("NULL"), r"\N")

    def test_quoted_string_unquoted(self):
        self.assertEqual(_sql_val_to_csv("'hello'"), "hello")

    def test_escaped_quote_unescaped(self):
        self.assertEqual(_sql_val_to_csv("'it''s'"), "it's")

    def test_integer_passthrough(self):
        self.assertEqual(_sql_val_to_csv("123"), "123")

    def test_float_passthrough(self):
        self.assertEqual(_sql_val_to_csv("0.9500"), "0.9500")

    def test_array_passthrough(self):
        result = _sql_val_to_csv("[('MRN','abc')]")
        self.assertTrue(result.startswith("["))

    def test_empty_string_col(self):
        self.assertEqual(_sql_val_to_csv("''"), "")


# ---------------------------------------------------------------------------
# write_gzipped_csv
# ---------------------------------------------------------------------------

class TestWriteGzippedCsv(unittest.TestCase):

    def test_creates_csv_gz_files(self):
        gen_fn = TABLES["configuration_audit_events"]
        with tempfile.TemporaryDirectory() as tmpdir:
            results = write_gzipped_csv("configuration_audit_events", gen_fn, 50, tmpdir)
        self.assertTrue(len(results) > 0)
        for path, count, elapsed in results:
            self.assertTrue(path.endswith(".csv.gz"))
            self.assertGreater(count, 0)
            self.assertGreater(elapsed, 0)

    def test_file_names_contain_table_and_event_type(self):
        gen_fn = TABLES["configuration_audit_events"]
        with tempfile.TemporaryDirectory() as tmpdir:
            results = write_gzipped_csv("configuration_audit_events", gen_fn, 20, tmpdir)
        for path, _, _ in results:
            fname = os.path.basename(path)
            self.assertTrue(fname.startswith("configuration_audit_events__"))

    def test_row_counts_sum_to_requested(self):
        gen_fn = TABLES["photo_events"]
        with tempfile.TemporaryDirectory() as tmpdir:
            results = write_gzipped_csv("photo_events", gen_fn, 100, tmpdir)
        total = sum(count for _, count, _ in results)
        self.assertEqual(total, 100)

    def test_csv_has_correct_headers(self):
        import csv as _csv
        import gzip as _gzip
        gen_fn = TABLES["hl7_outbound_events"]
        with tempfile.TemporaryDirectory() as tmpdir:
            results = write_gzipped_csv("hl7_outbound_events", gen_fn, 10, tmpdir)
            path = results[0][0]
            with _gzip.open(path, "rt", encoding="utf-8") as f:
                reader = _csv.reader(f)
                headers = next(reader)
        self.assertIn("metadata_eventId", headers)
        self.assertIn("photo_upload_outbound_event_id", headers)

    def test_multi_event_type_table_creates_multiple_files(self):
        gen_fn = TABLES["patient_audit_events"]
        with tempfile.TemporaryDirectory() as tmpdir:
            results = write_gzipped_csv("patient_audit_events", gen_fn, 300, tmpdir)
        self.assertGreater(len(results), 1)


if __name__ == "__main__":
    unittest.main()
