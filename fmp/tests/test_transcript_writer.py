from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.corpus.db import open_corpus_db
from core.corpus.ingest import ingest_raw
import core.corpus.validation as corpus_validation
from fmp.tools import transcripts
from fmp.tools.transcripts import _build_transcript_body


def test_returns_body_and_metadata(tmp_path) -> None:
    body, metadata = _build_transcript_body(_sample_result())

    assert body.startswith('# MSFT Earnings Call - Q1 FY2025')
    assert metadata['source'] == 'fmp_transcripts'
    assert metadata['form_type'] == 'TRANSCRIPT'
    assert list(tmp_path.iterdir()) == []


def test_no_exchange_headers() -> None:
    body, _ = _build_transcript_body(_sample_result())

    assert '### EXCHANGE' not in body
    assert '### SPEAKER: Keith Weiss (Analyst)' in body
    assert '### SPEAKER: Satya Nadella (CEO)' in body


def test_speaker_order_preserved() -> None:
    body, _ = _build_transcript_body(_sample_result())

    analyst_index = body.index('### SPEAKER: Keith Weiss (Analyst)')
    ceo_index = body.index('### SPEAKER: Satya Nadella (CEO)', analyst_index)
    cfo_index = body.index('### SPEAKER: Amy Hood (CFO)', ceo_index)

    assert analyst_index < ceo_index < cfo_index


def test_metadata_document_id_format() -> None:
    _, metadata = _build_transcript_body(_sample_result())

    assert metadata['document_id'] == 'fmp_transcripts:MSFT_2025-Q1'


def test_role_conditional() -> None:
    body, _ = _build_transcript_body(
        {
            'symbol': 'MSFT',
            'quarter': 1,
            'year': 2025,
            'date': '2025-01-29',
            'metadata': {
                'total_word_count': 12,
                'num_speakers': 2,
                'num_qa_exchanges': 0,
            },
            'prepared_remarks': [
                {'speaker': 'Jane Doe', 'role': '', 'text': 'No role.'},
                {'speaker': 'Jane Doe', 'role': 'CEO', 'text': 'Has role.'},
            ],
            'qa': [],
            'qa_exchanges': [],
        }
    )

    assert '### SPEAKER: Jane Doe\nNo role.' in body
    assert '### SPEAKER: Jane Doe (CEO)\nHas role.' in body


def test_via_ingest_raw(tmp_path) -> None:
    body, metadata = _build_transcript_body(_sample_result())
    corpus_root = tmp_path / 'corpus'
    db = open_corpus_db(tmp_path / 'corpus.sqlite3')

    result = ingest_raw(body, metadata, corpus_root, db)
    row = db.execute(
        'SELECT document_id, file_path FROM documents WHERE document_id = ?',
        ('fmp_transcripts:MSFT_2025-Q1',),
    ).fetchone()

    assert result.canonical_path.exists()
    assert row['file_path'] == str(result.canonical_path)
    assert Path(row['file_path']).read_text(encoding='utf-8').startswith('---\n')
    db.close()


def test_current_operating_transcript_stamps_cik(
    tmp_path,
    monkeypatch,
) -> None:
    profile_dir = tmp_path / 'profiles'
    profile_dir.mkdir()
    (profile_dir / 'MSFT.json').write_text(
        json.dumps({'cik': '789019', 'isEtf': False}),
        encoding='utf-8',
    )
    monkeypatch.setattr(corpus_validation, 'corpus_cik_cache_dir', lambda: profile_dir)
    monkeypatch.setattr(corpus_validation, '_UNIVERSE_FILES', ())
    body, metadata = _build_transcript_body(
        _sample_result(date=datetime.now(UTC).date().isoformat())
    )
    corpus_root = tmp_path / 'corpus'
    db = open_corpus_db(tmp_path / 'corpus.sqlite3')

    ingest_raw(body, metadata, corpus_root, db)
    row = db.execute(
        'SELECT cik FROM documents WHERE document_id = ?',
        ('fmp_transcripts:MSFT_2025-Q1',),
    ).fetchone()

    assert row['cik'] == '0000789019'
    db.close()


def test_current_etf_transcript_does_not_stamp_trust_cik(
    tmp_path,
    monkeypatch,
) -> None:
    profile_dir = tmp_path / 'profiles'
    profile_dir.mkdir()
    (profile_dir / 'SPY.json').write_text(
        json.dumps({'cik': '0000884394', 'isEtf': True}),
        encoding='utf-8',
    )
    monkeypatch.setattr(corpus_validation, 'corpus_cik_cache_dir', lambda: profile_dir)
    monkeypatch.setattr(corpus_validation, '_UNIVERSE_FILES', ())
    body, metadata = _build_transcript_body(
        _sample_result(symbol='SPY', date=datetime.now(UTC).date().isoformat())
    )
    corpus_root = tmp_path / 'corpus'
    db = open_corpus_db(tmp_path / 'corpus.sqlite3')

    ingest_raw(body, metadata, corpus_root, db)
    row = db.execute(
        'SELECT cik FROM documents WHERE document_id = ?',
        ('fmp_transcripts:SPY_2025-Q1',),
    ).fetchone()

    assert row['cik'] is None
    db.close()


def test_historical_transcript_does_not_stamp_cik(
    tmp_path,
    monkeypatch,
) -> None:
    profile_dir = tmp_path / 'profiles'
    profile_dir.mkdir()
    (profile_dir / 'MSFT.json').write_text(
        json.dumps({'cik': '789019', 'isEtf': False}),
        encoding='utf-8',
    )
    monkeypatch.setattr(corpus_validation, 'corpus_cik_cache_dir', lambda: profile_dir)
    monkeypatch.setattr(corpus_validation, '_UNIVERSE_FILES', ())
    historical_date = (datetime.now(UTC).date() - timedelta(days=400)).isoformat()
    body, metadata = _build_transcript_body(_sample_result(date=historical_date))
    corpus_root = tmp_path / 'corpus'
    db = open_corpus_db(tmp_path / 'corpus.sqlite3')

    ingest_raw(body, metadata, corpus_root, db)
    row = db.execute(
        'SELECT cik FROM documents WHERE document_id = ?',
        ('fmp_transcripts:MSFT_2025-Q1',),
    ).fetchone()

    assert row['cik'] is None
    db.close()


def test_get_earnings_transcript_env_ingests_canonical_full_transcript(
    tmp_path,
    monkeypatch,
) -> None:
    cache_path = tmp_path / 'parsed.json'
    cache_path.write_text(json.dumps(_sample_result()), encoding='utf-8')
    corpus_root = tmp_path / 'corpus'
    db_path = tmp_path / 'corpus.sqlite3'

    monkeypatch.setattr(transcripts, '_get_cache_path', lambda symbol, year, quarter: cache_path)
    monkeypatch.setenv('CORPUS_INGEST_ENABLED', 'true')
    monkeypatch.setenv('CORPUS_ROOT', str(corpus_root))
    monkeypatch.setenv('CORPUS_DB_PATH', str(db_path))

    response = transcripts.get_earnings_transcript(
        symbol='MSFT',
        year=2025,
        quarter=1,
        format='full',
        output='file',
    )

    db = open_corpus_db(db_path)
    row = db.execute(
        'SELECT document_id, file_path FROM documents WHERE document_id = ?',
        ('fmp_transcripts:MSFT_2025-Q1',),
    ).fetchone()

    assert response['status'] == 'success'
    assert response['file_path'] == str(row['file_path'])
    assert response['file_path'].startswith(str(corpus_root))
    assert Path(response['file_path']).exists()
    db.close()


def test_get_earnings_transcript_filtered_file_is_scratch_when_env_enabled(
    tmp_path,
    monkeypatch,
) -> None:
    cache_path = tmp_path / 'parsed.json'
    cache_path.write_text(json.dumps(_sample_result()), encoding='utf-8')
    legacy_dir = tmp_path / 'legacy-output'
    corpus_root = tmp_path / 'corpus'
    db_path = tmp_path / 'corpus.sqlite3'

    monkeypatch.setattr(transcripts, '_get_cache_path', lambda symbol, year, quarter: cache_path)
    monkeypatch.setattr(transcripts, 'FILE_OUTPUT_DIR', legacy_dir)
    monkeypatch.setenv('CORPUS_INGEST_ENABLED', 'true')
    monkeypatch.setenv('CORPUS_ROOT', str(corpus_root))
    monkeypatch.setenv('CORPUS_DB_PATH', str(db_path))

    response = transcripts.get_earnings_transcript(
        symbol='MSFT',
        year=2025,
        quarter=1,
        section='qa',
        format='full',
        output='file',
    )

    assert response['status'] == 'success'
    assert response['file_path'].startswith(str(legacy_dir))
    assert Path(response['file_path']).exists()
    assert not corpus_root.exists()

    db = open_corpus_db(db_path)
    try:
        row = db.execute(
            'SELECT COUNT(*) AS count FROM documents WHERE document_id = ?',
            ('fmp_transcripts:MSFT_2025-Q1',),
        ).fetchone()
    finally:
        db.close()
    assert row['count'] == 0


def _sample_result(
    *,
    symbol: str = 'MSFT',
    year: int = 2025,
    quarter: int = 1,
    date: str = '2025-01-29',
) -> dict:
    return {
        'symbol': symbol,
        'quarter': quarter,
        'year': year,
        'date': date,
        'metadata': {
            'total_word_count': 1234,
            'num_speakers': 4,
            'num_qa_exchanges': 1,
        },
        'prepared_remarks': [
            {'speaker': 'Satya Nadella', 'role': 'CEO', 'text': 'Welcome everyone.'},
            {'speaker': 'Amy Hood', 'role': 'CFO', 'text': 'Financial overview.'},
        ],
        'qa': [],
        'qa_exchanges': [
            {
                'analyst': 'Keith Weiss',
                'firm': 'Morgan Stanley',
                'question': 'What changed this quarter?',
                'answers': [
                    {'speaker': 'Satya Nadella', 'role': 'CEO', 'text': 'Demand improved.'},
                    {'speaker': 'Amy Hood', 'role': 'CFO', 'text': 'Margins expanded.'},
                ],
            }
        ],
    }
