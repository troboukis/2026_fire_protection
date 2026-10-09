import json
from types import SimpleNamespace

from scripts import classify_fire_prevention_batch as batch
import pytest


def document(ident=1, title='Ασαφές αντικείμενο'):
    return {'source': 'procurement', 'id': ident, 'reference_number': f'REF{ident}',
            'title': title, 'short_descriptions': ''}


def response(cid, evidence='', field=None):
    candidate = {'prevention': 2, 'reason': 'Ανεπαρκή στοιχεία',
                 'evidence': evidence, 'evidence_field': field}
    return {'custom_id': cid, 'response': {'status_code': 200, 'body': {
        'id': 'resp_test', 'model': 'actual-model', 'status': 'completed',
        'usage': {'input_tokens': 10}, 'output': [{'type': 'message', 'content': [
            {'type': 'output_text', 'text': json.dumps(candidate)}]}]}}}


def test_prepare_filters_keywords_and_model_input():
    entries, requests = batch.prepare([document(1, 'αποψίλωση'), document(2)], 'model')
    assert entries['procurement:1']['result']['prevention'] == 1
    assert len(requests) == 1
    assert requests[0]['custom_id'] == 'procurement:2'
    assert json.loads(requests[0]['body']['input']) == {'title': 'Ασαφές αντικείμενο'}


def test_rejected_evidence_retains_audit():
    audit = {}
    with pytest.raises(ValueError):
        batch.parse_batch_row(response('procurement:1', 'invented', 'title'), document(), audit)
    assert audit['response_id'] == 'resp_test'
    assert audit['model_candidate']['evidence'] == 'invented'


def test_http_failure_is_not_a_classification():
    with pytest.raises(ValueError):
        batch.parse_batch_row({'response': {'status_code': 429, 'body': {}}}, document(), {})


def test_collect_out_of_order_preview_never_connects_to_db(tmp_path, monkeypatch, capsys):
    entries, _ = batch.prepare([document(1), document(2)], 'model')
    state = {'batch_id': 'batch_test', 'model': 'model', 'prompt_version': 'test', 'entries': entries}
    rows = [response('procurement:2'), response('procurement:1')]
    client = SimpleNamespace(
        batches=SimpleNamespace(retrieve=lambda _: SimpleNamespace(
            status='completed', output_file_id='file_test', error_file_id=None)),
        files=SimpleNamespace(content=lambda _: SimpleNamespace(text='\n'.join(map(json.dumps, rows)))))
    monkeypatch.setattr(batch, 'AUDIT', tmp_path/'audit.jsonl')
    monkeypatch.setattr(batch.psycopg2, 'connect', lambda *a, **k: pytest.fail('Preview wrote to database'))
    assert batch.collect(client, SimpleNamespace(apply=False, state=tmp_path/'state.json'), state) == 0
    audit = [json.loads(line) for line in batch.AUDIT.read_text().splitlines()]
    assert [r['identifier'] for r in audit] == ['REF1', 'REF2']
    assert all(r['status'] == 'preview' for r in audit)


def test_incomplete_batch_does_not_process_results(tmp_path, monkeypatch):
    client = SimpleNamespace(batches=SimpleNamespace(retrieve=lambda _: SimpleNamespace(status='in_progress')))
    monkeypatch.setattr(batch.psycopg2, 'connect', lambda *a, **k: pytest.fail('Premature database access'))
    assert batch.collect(client, SimpleNamespace(apply=True), {'batch_id': 'batch_test'}) == 0


def test_local_only_collect_needs_no_openai_credentials(tmp_path, monkeypatch):
    entries, requests = batch.prepare([document(1, 'αποψίλωση')], 'model')
    assert not requests
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    monkeypatch.setattr(batch, 'OpenAI', lambda **kwargs: pytest.fail('Unnecessary OpenAI client'))
    monkeypatch.setattr(batch, 'AUDIT', tmp_path/'audit.jsonl')
    state = {'batch_id': None, 'phase': 'local_only', 'model': 'model',
             'prompt_version': 'test', 'entries': entries}
    assert batch.collect(None, SimpleNamespace(apply=False), state) == 0
    assert json.loads(batch.AUDIT.read_text())['prevention'] == 1


def test_local_only_status_needs_no_openai_credentials(tmp_path, monkeypatch, capsys):
    state = tmp_path/'state.json'
    state.write_text(json.dumps({'batch_id': None, 'phase': 'local_only'}))
    monkeypatch.setattr(batch, 'OpenAI', lambda **kwargs: pytest.fail('Unnecessary OpenAI client'))
    monkeypatch.setattr(batch, 'load_dotenv', lambda *args: None)
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    monkeypatch.setattr('sys.argv', ['batch', 'status', '--state', str(state)])
    assert batch.main() == 0
    assert 'local_only' in capsys.readouterr().out


def test_recovery_reattaches_remote_job_without_create(tmp_path):
    path = tmp_path/'state.json'
    state = {'phase': 'submitting', 'input_file_id': 'file_test', 'batch_id': None, 'entries': {}}
    job = SimpleNamespace(id='batch_recovered', input_file_id='file_test', endpoint='/v1/responses')
    client = SimpleNamespace(batches=SimpleNamespace(list=lambda **kwargs: iter([
        SimpleNamespace(id='unrelated', input_file_id='other', endpoint='/v1/responses'), job])))
    batch.recover_submission(client, path, state)
    assert state['batch_id'] == 'batch_recovered'
    assert json.loads(path.read_text())['batch_id'] == 'batch_recovered'


@pytest.mark.parametrize('number', [0, 2])
def test_recovery_never_guesses_or_resubmits(tmp_path, number):
    state = {'phase': 'submitting', 'input_file_id': 'file_test', 'batch_id': None}
    jobs = [SimpleNamespace(id=str(i), input_file_id='file_test', endpoint='/v1/responses')
            for i in range(number)]
    client = SimpleNamespace(batches=SimpleNamespace(list=lambda **kwargs: iter(jobs)))
    with pytest.raises(ValueError):
        batch.recover_submission(client, tmp_path/'state.json', state)
    assert state['batch_id'] is None
    assert not (tmp_path/'state.json').exists()


def test_recovery_save_failure_can_be_retried(tmp_path, monkeypatch):
    state = {'phase': 'submitting', 'input_file_id': 'file_test', 'batch_id': None}
    job = SimpleNamespace(id='batch_recovered', input_file_id='file_test', endpoint='/v1/responses')
    client = SimpleNamespace(batches=SimpleNamespace(list=lambda **kwargs: iter([job])))
    real_save = batch.save_state
    monkeypatch.setattr(batch, 'save_state', lambda *a: (_ for _ in ()).throw(OSError('disk failure')))
    with pytest.raises(OSError):
        batch.recover_submission(client, tmp_path/'state.json', state)
    assert state['batch_id'] is None
    monkeypatch.setattr(batch, 'save_state', real_save)
    batch.recover_submission(client, tmp_path/'state.json', state)
    assert state['batch_id'] == 'batch_recovered'
