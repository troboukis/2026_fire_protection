"""Offline budget/placeholder tests. No credentials or network requests."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import requests

spec = importlib.util.spec_from_file_location('deepl_ui', Path(__file__).with_name('deepl_ui.py'))
ui = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ui)

USAGE = {'character_count': 0, 'character_limit': 1_000_000}
GLOSSARY = {'protected_tokens': ['FireWatch'], 'entries': []}


class Response:
    def __init__(self, payload, status=200):
        self.payload, self.status_code = payload, status

    def json(self):
        return self.payload


class Session:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if not self.responses:
            raise AssertionError('Unexpected extra API call (possible retry)')
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.directory = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def client(self, responses):
        session = Session(responses)
        return ui.Client('offline-test:fx', self.directory, session), session

    def items(self, client, source='Σύνολο {{count, number}} EUR'):
        return client.items({'total': source}, 'UI labels only', GLOSSARY)

    def translated(self, items, billed=10):
        return Response({'translations': [{'text': item['wire'].replace('Σύνολο', 'Total'), 'billed_characters': billed} for item in items]})

    def test_unicode_estimate_uses_code_points_not_utf8_bytes(self):
        wire, _ = ui.protect('α🔥', [])
        self.assertEqual(len(wire), 9)
        self.assertGreater(len(wire.encode()), len(wire))

    def test_protects_placeholders_urls_numbers_brands_and_component_markers(self):
        text = 'FireWatch: {{count, number}} / 2026 <0>πηγή</0> https://example.test/?q=1&x=2'
        wire, protected = ui.protect(text, ['FireWatch'])
        self.assertEqual(ui.restore(wire, protected), text)
        self.assertEqual(len(protected), 6)

    def test_reordered_protected_values_are_valid(self):
        self.assertEqual(ui.restore('<s><x id="1">2026</x> <x id="0">{{count}}</x></s>', ['{{count}}', '2026']), '2026 {{count}}')

    def test_altered_or_missing_or_duplicate_placeholder_is_rejected(self):
        for value in ('<s><x id="0">{{n}}</x></s>', '<s>lost</s>', '<s><x id="0">{{count}}</x><x id="0">{{count}}</x></s>'):
            with self.subTest(value=value), self.assertRaises(ui.TranslationError):
                ui.restore(value, ['{{count}}'])

    def test_invalid_xml_and_unexpected_tags_are_rejected(self):
        for value in ('<s>broken', '<s><unexpected>value</unexpected></s>'):
            with self.subTest(value=value), self.assertRaises(ui.TranslationError):
                ui.restore(value, [])

    def test_invalid_or_incomplete_usage_fails_closed(self):
        for usage in ({}, {'character_count': True, 'character_limit': 10}, {**USAGE, 'api_key_character_limit': 10}):
            client, session = self.client([Response(usage)])
            with self.assertRaises(ui.TranslationError):
                client.translate_batch(self.items(client), 'UI only', 'glossary-id')
            self.assertEqual([c[0] for c in session.calls], ['GET'])

    def test_no_post_if_usage_connection_fails(self):
        client, session = self.client([requests.ConnectionError()])
        with self.assertRaises(ui.TranslationError):
            client.translate_batch(self.items(client), 'UI only', 'glossary-id')
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(client.state['requests'], [])

    def test_budget_exhausted_means_no_post(self):
        client, session = self.client([Response({'character_count': 999_999, 'character_limit': 1_000_000})])
        with self.assertRaises(ui.TranslationError):
            client.translate_batch(self.items(client), 'UI only', 'glossary-id')
        self.assertEqual([c[0] for c in session.calls], ['GET'])

    def test_lower_key_limit_is_enforced(self):
        usage = {**USAGE, 'api_key_character_count': 90, 'api_key_character_limit': 100}
        client, session = self.client([Response(usage)])
        with self.assertRaises(ui.TranslationError):
            client.translate_batch(self.items(client), 'UI only', 'glossary-id')
        self.assertEqual(len(session.calls), 1)

    def test_project_limit_survives_account_reset(self):
        state = {'requests': [{'status': 'complete', 'billed': 999_999, 'reserved': 999_999}]}
        self.assertEqual(ui.capacity(USAGE, state), 1)

    def test_lagging_usage_does_not_release_local_budget(self):
        state = {'requests': [{'status': 'complete', 'billed': 70, 'reserved': 70}]}
        self.assertEqual(ui.capacity({'character_count': 0, 'character_limit': 100}, state), 30)
        # A reflected remote count is deliberately double counted until review.
        self.assertEqual(ui.capacity({'character_count': 70, 'character_limit': 100}, state), 0)

    def test_reservation_is_durable_before_post(self):
        client, session = self.client([])
        items = self.items(client)
        original = session.request
        session.responses = [Response(USAGE), self.translated(items), Response(USAGE)]
        def check(method, url, **kwargs):
            if method == 'POST':
                ledger = json.loads(client.path.read_text())
                self.assertEqual(ledger['requests'][-1]['status'], 'reserved')
                self.assertGreater(ledger['requests'][-1]['reserved'], 0)
            return original(method, url, **kwargs)
        session.request = check
        self.assertEqual(client.translate_batch(items, 'UI only', 'glossary-id'), 10)
        self.assertEqual(client.state['requests'][0]['status'], 'complete')
        self.assertIn(items[0]['identity'], client.state['cache'])

    def test_ambiguous_timeout_retains_reservation_and_blocks_resume(self):
        client, session = self.client([Response(USAGE), requests.Timeout(), Response(USAGE)])
        with self.assertRaises(ui.TranslationError):
            client.translate_batch(self.items(client), 'UI only', 'glossary-id')
        self.assertEqual([c[0] for c in session.calls], ['GET', 'POST', 'GET'])
        reloaded = ui.Client('offline-test:fx', self.directory, Session([]))
        self.assertEqual(reloaded.state['requests'][0]['status'], 'uncertain')
        with self.assertRaises(ui.TranslationError):
            reloaded.ensure_clear()

    def test_server_error_is_not_automatically_retried(self):
        client, session = self.client([Response(USAGE), Response({}, 503), Response(USAGE)])
        with self.assertRaises(ui.TranslationError):
            client.translate_batch(self.items(client), 'UI only', 'glossary-id')
        self.assertEqual(sum(method == 'POST' for method, *_ in session.calls), 1)

    def test_usage_failure_after_success_keeps_paid_output_cached(self):
        client, session = self.client([])
        items = self.items(client)
        session.responses = [Response(USAGE), self.translated(items), requests.Timeout()]
        with self.assertRaises(ui.TranslationError):
            client.translate_batch(items, 'UI only', 'glossary-id')
        self.assertEqual(client.state['requests'][0]['status'], 'complete')
        self.assertIn(items[0]['identity'], client.state['cache'])
        reloaded = ui.Client('offline-test:fx', self.directory, Session([]))
        reloaded.ensure_clear()
        self.assertIn(items[0]['identity'], reloaded.state['cache'])

    def test_changed_placeholder_or_missing_billing_keeps_raw_response(self):
        for translation in ({'text': '<s>Lost</s>', 'billed_characters': 10}, {'text': '<s>Value</s>'}):
            with tempfile.TemporaryDirectory() as directory:
                session = Session([Response(USAGE), Response({'translations': [translation]}), Response(USAGE)])
                client = ui.Client('offline-test:fx', Path(directory), session)
                with self.assertRaises(ui.TranslationError):
                    client.translate_batch(self.items(client), 'UI only', 'glossary-id')
                record = client.state['requests'][0]
                self.assertEqual(record['status'], 'uncertain')
                self.assertEqual(record['response']['translations'][0], translation)
                self.assertEqual(client.state['cache'], {})

    def test_cache_identity_includes_context_glossary_and_options(self):
        client, _ = self.client([])
        a = self.items(client)[0]['identity']
        self.assertEqual(a, self.items(client)[0]['identity'])
        self.assertNotEqual(a, client.items({'total': 'Σύνολο {{count, number}} EUR'}, 'Different context', GLOSSARY)[0]['identity'])
        self.assertNotEqual(a, client.items({'total': 'Σύνολο {{count, number}} EUR'}, 'UI labels only', {**GLOSSARY, 'version': 2})[0]['identity'])

    def test_glossary_notes_and_status_do_not_retranslate_paid_text(self):
        client, _ = self.client([])
        messages, context = {'value': 'Ποσό'}, 'UI labels'
        original = client.items(messages, context, GLOSSARY)[0]['identity']
        metadata = {**GLOSSARY, 'status': 'active', 'notes': ['Editorial note only']}
        self.assertEqual(original, client.items(messages, context, metadata)[0]['identity'])
        changed = {**GLOSSARY, 'entries': [{'source': 'Ποσό', 'target': 'Amount'}]}
        self.assertNotEqual(original, client.items(messages, context, changed)[0]['identity'])

    def test_legacy_cache_is_migrated_without_a_post(self):
        client, session = self.client([])
        source, context = 'Ποσό', 'UI labels'
        old = ui.fingerprint({'source': source, 'context': context, 'glossary': GLOSSARY, 'options': ui.OPTIONS})
        client.state['cache'][old] = {'source': source, 'machine': 'Amount'}
        identity = client.items({'value': source}, context, GLOSSARY)[0]['identity']
        self.assertEqual(client.state['cache'][identity]['machine'], 'Amount')
        self.assertEqual(session.calls, [])

    def test_review_preserves_paid_machine_text_across_glossary_changes_without_post(self):
        client, session = self.client([])
        source, context = {'label': 'Σύνολο {{count}}'}, 'UI only'
        old = client.items(source, context, GLOSSARY)[0]
        client.state['cache'][old['identity']] = {'source': old['source'], 'machine': 'Total {{count}}'}
        client.items(source, context, GLOSSARY)  # verify and bind existing context/options
        updated = {**GLOSSARY, 'entries': [{'source': 'σύνολο', 'target': 'total'}]}
        new = client.items(source, context, updated)
        client.review(new, context, {'label': 'Reviewed total {{count}}'})
        cached = client.state['cache'][new[0]['identity']]
        self.assertEqual(cached['machine'], 'Total {{count}}')
        self.assertEqual(cached['reviewed'], 'Reviewed total {{count}}')
        self.assertEqual(client.state['requests'], [])
        self.assertEqual(session.calls, [])

    def test_review_rejects_untranslated_sources_other_contexts_and_changed_placeholders(self):
        client, _ = self.client([])
        items = self.items(client)
        with self.assertRaises(ui.TranslationError):
            client.review(items, 'UI only', {'total': 'Unpaid {{count, number}} EUR'})
        client.state['cache'][items[0]['identity']] = {'source': items[0]['source'], 'machine': 'Total {{count, number}} EUR'}
        client.items({'total': items[0]['source']}, 'UI labels only', GLOSSARY)
        with self.assertRaises(ui.TranslationError):
            client.review(items, 'different context', {'total': 'Total {{count, number}} EUR'})
        with self.assertRaises(ui.TranslationError):
            client.review(items, 'UI labels only', {'total': 'Missing placeholder EUR'})

    def test_updates_only_our_existing_glossary_and_keeps_a_snapshot(self):
        client, session = self.client([])
        client.state['glossaries']['previous-version'] = 'owned-id'
        glossary = {**GLOSSARY, 'entries': [{'source': 'σύνδεσμος δήμων', 'target': 'Association of Municipalities'}]}
        before = {'dictionaries': [{'entries': 'παλιό\told'}]}
        after = {'dictionaries': [{'entries': 'σύνδεσμος δήμων\tAssociation of Municipalities'}]}
        session.responses = [Response({'glossaries': [{'glossary_id': 'owned-id', 'name': 'FireWatch EL-EN previous',
                             'dictionaries': [{'source_lang': 'el', 'target_lang': 'en'}]}]}),
                             Response(before), Response({}), Response(after)]
        self.assertEqual(client.glossary(glossary), 'owned-id')
        self.assertEqual([method for method, *_ in session.calls], ['GET', 'GET', 'PUT', 'GET'])
        self.assertEqual(client.state['glossary_updates'][0]['before'], before)
        self.assertEqual(client.state['glossary_updates'][0]['status'], 'complete')
        self.assertNotIn('previous-version', client.state['glossaries'])

    def test_never_updates_an_unrecognized_glossary(self):
        client, session = self.client([])
        session.responses = [Response({'glossaries': [{'glossary_id': 'someone-elses-id', 'name': 'FireWatch EL-EN other'}]}),
                             Response({}, 456)]
        with self.assertRaises(ui.TranslationError):
            client.glossary(GLOSSARY)
        self.assertNotIn('PUT', [method for method, *_ in session.calls])

    def test_lock_prevents_concurrent_run(self):
        with ui.locked(self.directory):
            with self.assertRaises(ui.TranslationError):
                with ui.locked(self.directory):
                    self.fail('Concurrent run acquired lock')

    def test_changing_key_cannot_reset_project_ledger(self):
        self.client([])
        with self.assertRaises(ui.TranslationError):
            ui.Client('a-different-key', self.directory, Session([]))

    def test_arbitrary_files_and_source_title_catalogues_are_not_accepted(self):
        for name in ('contract_titles', 'document_titles', '../../data/source.json'):
            with self.subTest(name=name), self.assertRaises(ui.TranslationError):
                ui.read_catalogue(name)

    def test_no_credentials_in_ledger_and_no_redirects(self):
        client, session = self.client([Response(USAGE)])
        client.usage()
        self.assertNotIn('offline-test:fx', client.path.read_text())
        self.assertFalse(session.calls[0][2]['allow_redirects'])

    def test_cached_catalogue_sends_no_glossary_or_translation_post(self):
        client, _ = self.client([])
        messages, context = {'greeting': 'Γεια'}, 'UI only'
        item = client.items(messages, context, GLOSSARY)[0]
        client.state['cache'][item['identity']] = {'source': 'Γεια', 'machine': 'Hello'}
        client.save()
        source, glossary = self.directory / 'ui.json', self.directory / 'glossary.json'
        source.write_text(json.dumps(messages))
        glossary.write_text(json.dumps(GLOSSARY))
        session = Session([Response(USAGE), Response(USAGE)])
        definitions = {'test': {'path': source, 'context': context, 'kind': 'ui'}}
        with patch.object(ui, 'CATALOGUES', definitions), patch.object(ui, 'GLOSSARY', glossary), patch.object(ui.requests, 'Session', return_value=session):
            report = ui.run('translate', 'test', 'offline-test:fx', 1, self.directory)
        self.assertEqual(report['uncached_unique_messages'], 0)
        self.assertEqual(report['conservative_available'], 1_000_000)
        self.assertEqual([method for method, *_ in session.calls], ['GET', 'GET'])
        self.assertEqual(json.loads((self.directory / 'test.machine.json').read_text()), {'greeting': 'Hello'})


if __name__ == '__main__':
    unittest.main()
