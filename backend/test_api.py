"""Comprehensive API smoke tests for the running backend service.

Usage:
  python backend/test_api.py

Optional environment variables:
  SAYPY_BASE_URL   (default: http://127.0.0.1:8000)
  SAYPY_USERNAME   (for authenticated smoke tests)
  SAYPY_PASSWORD   (for authenticated smoke tests)
  SAYPY_API_KEY    (alternative auth path for some endpoints)

Notes:
  - These tests are integration-style and hit a live server.
  - They prefer non-destructive checks. Some create/update endpoints are verified
    via auth/validation behavior rather than persistent writes.
"""

from __future__ import annotations

import json
import os
import unittest
import urllib.error
import urllib.parse
import urllib.request


BASE = os.getenv('SAYPY_BASE_URL', 'http://127.0.0.1:8000').rstrip('/')
USERNAME = os.getenv('SAYPY_USERNAME', '').strip()
PASSWORD = os.getenv('SAYPY_PASSWORD', '').strip()
API_KEY = os.getenv('SAYPY_API_KEY', '').strip()


def _http_json(path: str, method: str = 'GET', data=None, headers=None):
    url = f"{BASE}{path}"
    payload = None
    req_headers = {'Accept': 'application/json'}
    if headers:
        req_headers.update(headers)

    if data is not None:
        payload = json.dumps(data).encode('utf-8')
        req_headers.setdefault('Content-Type', 'application/json')

    req = urllib.request.Request(url, data=payload, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode('utf-8')
            body = None
            try:
                body = json.loads(raw) if raw else None
            except Exception:
                body = raw
            return resp.getcode(), body, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode('utf-8')
        body = None
        try:
            body = json.loads(raw) if raw else None
        except Exception:
            body = raw
        return e.code, body, raw


def _auth_headers(token: str | None = None):
    headers = {}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    if API_KEY:
        headers['x-api-key'] = API_KEY
    return headers


class ApiSmokeTests(unittest.TestCase):
    token = None

    @classmethod
    def setUpClass(cls):
        # Login is optional for the whole suite; auth-required tests can skip if absent.
        cls.token = None
        if USERNAME and PASSWORD:
            code, body, _ = _http_json('/users/login', method='POST', data={'username': USERNAME, 'password': PASSWORD})
            if code == 200 and isinstance(body, dict):
                cls.token = body.get('token')

    def test_health_public_endpoints(self):
        # Public endpoints should respond (success or expected server-config error), not 404.
        for path in ['/config', '/churches']:
            code, _, _ = _http_json(path)
            self.assertNotEqual(code, 404, msg=f'{path} should exist')

    def test_auth_guard_on_user_profile_routes(self):
        for path in ['/users', '/roles', '/uploaders', '/collection_codes']:
            code, _, _ = _http_json(path)
            self.assertIn(code, {401, 403}, msg=f'{path} should be guarded without auth')

    def test_auth_guard_on_key_or_user_routes(self):
        guarded = [
            '/members',
            '/members_view',
            '/reports/members_collections',
            '/reports/period_summary',
            '/members_collection/delete_requests',
        ]
        for path in guarded:
            code, _, _ = _http_json(path)
            self.assertEqual(code, 401, msg=f'{path} should require API key or bearer token')

    def test_validate_members_collections_requires_auth(self):
        rows = [{'collection_code': 'import', 's2': '2026-04-18T00:00:00', 's3': 1, 's4': 'Tester'}]
        code, _, _ = _http_json('/members_collections/validate', method='POST', data=rows)
        self.assertEqual(code, 401)

    def test_submit_disallows_unknown_table(self):
        headers = _auth_headers(self.token)
        if not headers:
            self.skipTest('No credentials/API key provided for submit tests')
        code, body, _ = _http_json('/submit/not_a_table', method='POST', data={'x': 1}, headers=headers)
        self.assertEqual(code, 400)
        if isinstance(body, dict):
            self.assertIn('detail', body)

    def test_login_endpoint_shape(self):
        if not (USERNAME and PASSWORD):
            self.skipTest('Set SAYPY_USERNAME and SAYPY_PASSWORD to test login shape')
        code, body, _ = _http_json('/users/login', method='POST', data={'username': USERNAME, 'password': PASSWORD})
        self.assertIn(code, {200, 401})
        if code == 200:
            self.assertIsInstance(body, dict)
            self.assertIn('token', body)
            self.assertIn('user', body)

    def test_authenticated_read_smoke(self):
        headers = _auth_headers(self.token)
        if not headers:
            self.skipTest('Provide SAYPY_USERNAME/SAYPY_PASSWORD or SAYPY_API_KEY for authenticated tests')

        # Routes that should be reachable with valid auth, though role may still forbid some.
        paths = [
            '/reports/members_collections',
            '/reports/period_summary?start_date=2026-04-18&end_date=2026-04-18',
            '/members_view',
        ]
        for path in paths:
            code, _, _ = _http_json(path, headers=headers)
            self.assertIn(code, {200, 400, 403}, msg=f'unexpected status for {path}: {code}')

    def test_period_summary_date_formats(self):
        headers = _auth_headers(self.token)
        if not headers:
            self.skipTest('Provide auth to test period summary date parsing')

        date_pairs = [
            ('2026-04-18', '2026-04-18'),
            ('18/04/2026', '18/04/2026'),
        ]
        for s, e in date_pairs:
            qs = urllib.parse.urlencode({'start_date': s, 'end_date': e})
            code, body, _ = _http_json(f'/reports/period_summary?{qs}', headers=headers)
            self.assertIn(code, {200, 403}, msg=f'period_summary format {s} produced {code}')
            if code == 200:
                self.assertIsInstance(body, dict)
                self.assertIn('rows', body)
                self.assertIn('totals', body)

    def test_headers_upload_endpoint_rejects_missing_file(self):
        headers = _auth_headers(self.token)
        if not headers:
            self.skipTest('No credentials/API key provided for upload tests')
        # Wrong content-type/no file should fail with 4xx, but route should exist.
        code, _, _ = _http_json('/upload/headers', method='POST', data={'no_file': True}, headers=headers)
        self.assertIn(code, {400, 401, 403, 422})


if __name__ == '__main__':
    unittest.main(verbosity=2)
