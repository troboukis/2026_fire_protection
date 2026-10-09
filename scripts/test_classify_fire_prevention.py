import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import classify_fire_prevention_keywords as keywords

import pytest

spec = importlib.util.spec_from_file_location('classifier', Path(__file__).with_name('classify_fire_prevention.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def client_for(result, status='completed'):
    return SimpleNamespace(responses=SimpleNamespace(create=lambda **kwargs: SimpleNamespace(
        status=status, output_text=json.dumps(result))))


@pytest.mark.parametrize('value', [0, 1, 2])
def test_valid_classifications(value):
    result = {'prevention': value, 'reason': 'Αιτιολόγηση', 'evidence': 'πυρκαγιά', 'evidence_field': 'title'}
    assert module.classify(client_for(result), 'model', {'title': 'δασική πυρκαγιά'}) == result


def test_model_receives_only_nonempty_text_fields():
    captured = {}
    result = {'prevention': 2, 'reason': 'Ανεπαρκή στοιχεία', 'evidence': '', 'evidence_field': None}

    def create(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(status='completed', output_text=json.dumps(result))

    client = SimpleNamespace(responses=SimpleNamespace(create=create))
    document = {
        'id': 123, 'source': 'diavgeia', 'reference_number': 'REF', 'ada': 'ADA',
        'diavgeia_document_type_decision_uid': module.PAYMENT_TYPE,
        'title': 'Τίτλος', 'short_descriptions': '', 'subject': 'Θέμα',
        'unexpected_metadata': 'must not reach model',
    }
    module.classify(client, 'model', document)
    assert json.loads(captured['input']) == {'title': 'Τίτλος', 'subject': 'Θέμα'}


@pytest.mark.parametrize('result', [
    {'prevention': True, 'reason': 'r', 'evidence': 'πυρκαγιά', 'evidence_field': 'title'},
    {'prevention': None, 'reason': 'r', 'evidence': 'πυρκαγιά', 'evidence_field': 'title'},
    {'prevention': 0, 'reason': 'r', 'evidence': '', 'evidence_field': None},
    {'prevention': 1, 'reason': 'r', 'evidence': 'επινοημένο', 'evidence_field': 'title'},
])
def test_invalid_results_cannot_be_saved(result):
    with pytest.raises(ValueError):
        module.classify(client_for(result), 'model', {'title': 'πυρκαγιά'})


def test_incomplete_response_is_an_error():
    with pytest.raises(ValueError):
        module.classify(client_for({}), 'model', {'title': 'πυρκαγιά'})


def test_empty_text_is_unclassifiable_without_api_call():
    assert module.classify(None, 'model', {'subject': ''})['prevention'] == 2


def test_unclassifiable_result_is_saved_and_not_selected_again():
    class Connection:
        value = None
        rowcount = 0
        def cursor(self, **kwargs): return self
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params=None):
            if sql.startswith('UPDATE'):
                if self.value is None:
                    self.value = params[0]
                    self.rowcount = 1
                else:
                    self.rowcount = 0
            elif sql.startswith('SELECT'):
                self.rows = [] if self.value is not None else [document]
        def fetchall(self): return self.rows
        def commit(self): pass
        def rollback(self): pass
    document = {'source': 'procurement', 'id': 1, 'reference_number': 'REF',
                'title': 'Αμφίσημο', 'short_descriptions': ''}
    conn = Connection()
    assert module.apply_result(conn, document, {'prevention': 2}) == 'updated'
    assert conn.value == 2
    args = SimpleNamespace(source='procurement', reference_number=None, ada=None, limit_per_source=10)
    assert module.select_documents(conn, args) == []
    assert module.apply_result(conn, document, {'prevention': 0}) == 'skipped_changed_record'
    assert conn.value == 2


def test_quote_in_wrong_field_is_rejected():
    result = {'prevention': 1, 'reason': 'r', 'evidence': 'καθαρισμός',
              'evidence_field': 'subject'}
    with pytest.raises(ValueError):
        module.classify(client_for(result), 'model', {'title': 'καθαρισμός', 'subject': 'άλλο'})


def test_response_audit_is_kept_even_when_validation_fails():
    response = SimpleNamespace(status='completed', output_text='{}', id='resp_test',
                               model='actual-model', usage=SimpleNamespace(
                                   model_dump=lambda: {'input_tokens': 20, 'output_tokens': 5}))
    client = SimpleNamespace(responses=SimpleNamespace(create=lambda **kwargs: response))
    audit = {}
    with pytest.raises(ValueError):
        module.classify(client, 'requested-model', {'title': 'Κείμενο'}, audit)
    assert audit == {'response_id': 'resp_test', 'actual_model': 'actual-model',
                     'token_usage': {'input_tokens': 20, 'output_tokens': 5}, 'model_candidate': {}, 'keyword_rules_version': keywords.KEYWORD_RULES_VERSION,
                     'keyword_matches': [], 'classification_method': 'model'}


def test_both_sources_get_their_own_limit():
    class Connection:
        def cursor(self, **kwargs): return self
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params=None):
            if sql.startswith('SELECT'):
                self.rows = [{'id': i} for i in range(params[-1])]
        def fetchall(self): return self.rows
        def rollback(self): pass
    args = SimpleNamespace(source='both', reference_number=None, ada=None, limit_per_source=10)
    records = module.select_documents(Connection(), args)
    assert len(records) == 20
    assert sum(r['source'] == 'diavgeia' for r in records) == 10


def test_error_message_redacts_secrets_and_is_bounded(monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY', 'test-secret')
    message = module.safe_error(ValueError('test-secret Bearer abc ' + 'x' * 2000))
    assert 'test-secret' not in message and 'Bearer abc' not in message
    assert len(message) == 1000


def test_rejected_evidence_is_preserved_in_audit():
    result = {'prevention': 1, 'reason': 'r', 'evidence': 'ΔΙΟΡΘΩΜΕΝΟ ΚΕΙΜΕΝΟ',
              'evidence_field': 'title'}
    audit = {}
    with pytest.raises(ValueError, match='verbatim'):
        module.classify(client_for(result), 'model', {'title': 'αρχικό κείμενο'}, audit)
    assert audit['model_candidate'] == result
    assert 'prevention' not in audit


@pytest.mark.parametrize('status,message', [
    ('preview', 'δεν αποθηκεύτηκε στη βάση'),
    ('updated', 'Αποθηκεύτηκε στη βάση'),
    ('skipped_changed_record', 'η εγγραφή άλλαξε'),
])
def test_terminal_explains_result_and_storage(status, message):
    text = module.format_terminal_result({
        'source': 'procurement', 'identifier': 'REF', 'status': status,
        'prevention': 2, 'reason': 'Ανεπαρκή στοιχεία', 'evidence': '',
        'response_id': 'technical-response', 'prompt_version': 'technical-version',
    })
    assert 'Δεν μπορεί να ταξινομηθεί (2)' in text
    assert message in text
    assert 'technical-' not in text


def test_terminal_error_does_not_look_like_success():
    text = module.format_terminal_result({
        'source': 'diavgeia', 'identifier': 'ADA', 'status': 'error',
        'model_candidate': {'prevention': 1}, 'traceback': 'technical-stack',
    })
    assert 'απέτυχε' in text and 'Δεν αποθηκεύτηκε' in text
    assert 'Πρόληψη' not in text and 'technical-stack' not in text


@pytest.mark.parametrize('text,value', [
    ('Προμήθεια δεξαμενών πυρόσβεσης', 1), ('ΑΠΟΨΙΛΩΣΕΙΣ', 1),
    ('συντήρησης πρασίνου', 1), ('ΠΡΟΜΗΘΕΙΑ ΠΥΡΟΣΒΕΣΤΗΡΩΝ', 1),
    ('Συντηρήσεις πυροσβεστήρων', 1), ('εργασίες πρόληψης', 1),
    ('εργασιών καθαρισμού', 1), ('αντιπυρικές ζώνες', 1),
    ('πρόληψη δασικών πυρκαγιών', 1), ('καθαρισμού δρόμων', 1),
    ('διαπλάτυνση αντιπυρικών ζωνών', 1),
    ('συντήρηση ζωνών αντιπυρικής προστασίας', 1),
    ('αποκατάσταση αγροτικών οδών', 1), ('υπηρεσίες πρόληψης', 1),
    ('καθαρισμού δασικών δρόμων', 1), ('συντήρηση δασικών δρόμων', 1),
    ('καθαρισμού αγροτικών δρόμων', 1), ('συντήρηση αγροτικών δρόμων', 1),
    ('κοπή χόρτων', 1), ('καθαρισμού πρανών', 1),
    ('προληπτικός καθαρισμός', 1), ('δράσεις πυροπροστασίας', 1),
    ('καθαρισμός πρασίνου', 1), ('εργασίες κατάσβεσης', 0),
    ('ΚΑΤΕΠΕΙΓΟΥΣΑ ΜΙΣΘΩΣΗ', 0), ('κατεπείγουσας μίσθωσης', 0),
    ('έκτακτες εργασίες', 0),
    ('ΣυνΤήΡηση ΠραΣίνου', 1),
])
def test_keyword_classification_skips_api_and_keeps_exact_evidence(text, value):
    audit = {}
    result = module.classify(None, 'unused', {'subject': text}, audit)
    assert result['prevention'] == value
    assert result['evidence'] in text and result['evidence_field'] == 'subject'
    assert audit['classification_method'] == 'keywords'


def test_conflicting_categories_defer_to_model():
    assert module.classify_keywords({'title': 'αποψίλωση',
                                    'short_descriptions': 'εργασίες κατάσβεσης'}) is None


def test_roots_match_only_at_token_start_and_ignore_metadata():
    assert module.classify_keywords({'title': 'ψευδοαποψίλωση',
                                    'reference_number': 'αποψίλωση'}) is None


@pytest.mark.parametrize('word', ['έκτακτη', 'ΕΚΤΑΚΤΕΣ', 'επείγουσα', 'επειγουσών'])
def test_urgent_word_blocks_prevention_across_fields(word):
    audit = {}
    assert keywords.classify_keywords({'title': 'αποψίλωση', 'short_descriptions': word}, audit) is None
    assert audit['prevention_blockers']


def test_emergency_work_overrides_prevention_match():
    assert keywords.classify_keywords({'title': 'Έκτακτη εργασία αποψίλωσης'})['prevention'] == 0


def test_fire_response_phrase_no_longer_classifies_as_suppression():
    assert keywords.classify_keywords({'title': 'αντιμετώπιση πυρκαγιών'}) is None
    assert keywords.classify_keywords({'title': 'αντιμετώπιση πυρκαγιών και αποψίλωση'})['prevention'] == 1


@pytest.mark.parametrize('text', [
    'Συντήρησης – Βελτίωσης Δασικών Δρόμων',
    'συντήρηση και βελτίωση δασικών δρόμων',
    'προμήθεια πέντε πυροσβεστήρων',
])
def test_intervening_words_are_allowed_and_evidence_is_original(text):
    result = keywords.classify_keywords({'title': text})
    assert result['prevention'] == 1
    assert result['evidence'] in text


def test_gap_limit_and_word_order():
    roots = ('συντηρ', 'δασικ', 'δρομ')
    assert list(keywords.matching_spans(['συντηρηση', 'και', 'βελτιωση', 'δασικων', 'δρομων'], roots)) == [(0, 4)]
    assert list(keywords.matching_spans(['συντηρηση', 'α', 'β', 'γ', 'δασικων', 'δρομων'], roots)) == []
    assert list(keywords.matching_spans(['δασικων', 'δρομων', 'συντηρηση'], roots)) == []


def test_gap_matching_checks_alternative_paths():
    tokens = ['συντηρηση', 'δασικων', 'και', 'δασικων', 'των', 'των', 'δρομων']
    assert list(keywords.matching_spans(tokens, ('συντηρ', 'δασικ', 'δρομ'))) == [(0, 6)]


@pytest.mark.parametrize('method,message', [
    ('keywords', 'Τρόπος ταξινόμησης: Keywords.'),
    ('model', 'Τρόπος ταξινόμησης: OpenAI.'),
    ('empty_text', 'Χωρίς διαθέσιμο κείμενο'),
])
def test_terminal_displays_classification_method(method, message):
    text = module.format_terminal_result({
        'source': 'procurement', 'identifier': 'REF', 'status': 'preview',
        'prevention': 2, 'reason': 'Ανεπαρκή στοιχεία', 'evidence': '',
        'classification_method': method,
    })
    assert message in text
