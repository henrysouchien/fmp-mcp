from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sys
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research_corpus.db import open_corpus_db
from research_corpus.ingest import ingest_raw
import research_corpus.validation as corpus_validation
from fmp import file_residency
from fmp.tools import transcripts
from fmp.tools.transcripts import _build_transcript_body, parse_transcript


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


def test_parse_transcript_does_not_overwrite_explicit_role_with_ir_mention() -> None:
    parsed = parse_transcript(
        "\n".join(
            [
                (
                    "Operator: It is now my pleasure to introduce your host, Brett Iversen, "
                    "Vice President of Investor Relations."
                ),
                (
                    "Brett Iversen: On the call with me are Satya Nadella, Chairman and "
                    "Chief Executive Officer; Amy Hood, Chief Financial Officer. On the "
                    "Microsoft Investor Relations website, you can find our earnings release."
                ),
                "Satya Nadella: Thank you, Brett. This quarter we saw continued strength.",
                (
                    "Amy Hood: Thank you, Satya, and I want to congratulate Brett for his "
                    "leadership of Investor Relations."
                ),
                "Operator: We will now begin the question-and-answer session.",
                "Keith Weiss: Thank you.",
                "Amy Hood: Thanks, Keith. Let me spend a little time on that.",
            ]
        )
    )

    prepared_roles = {
        segment["speaker"]: segment["role"]
        for segment in parsed["prepared_remarks"]
    }
    qa_amy = [
        segment
        for segment in parsed["qa"]
        if segment["speaker"] == "Amy Hood"
    ]
    assert prepared_roles["Brett Iversen"] == "IR"
    assert prepared_roles["Satya Nadella"] == "CEO"
    assert prepared_roles["Amy Hood"] == "CFO"
    assert qa_amy[0]["role"] == "CFO"


def test_parse_transcript_preserves_period_delimited_payload_without_false_attribution() -> None:
    content = (
        "Operator. Welcome to the quarterly call. Elizabeth Shea. Thank you for "
        "joining us. Robert Michael. Revenue grew during the quarter. Operator. "
        "We will now begin questions. Chris Schott. Could you discuss the outlook?"
    )

    parsed = parse_transcript(content)

    assert parsed["qa"] == []
    assert parsed["qa_exchanges"] == []
    assert parsed["prepared_remarks"] == [
        {
            "speaker": "Unknown",
            "role": "Other",
            "text": content,
            "word_count": len(content.split()),
        }
    ]
    assert parsed["metadata"]["speaker_parse_fallback"] == "unsegmented"
    assert parsed["metadata"]["total_word_count"] == len(content.split())


def test_parse_transcript_repairs_systematic_constant_currency_percent_fusions() -> None:
    parsed = parse_transcript(
        "\n".join(
            [
                "Operator: Welcome to the call.",
                (
                    "Amy Hood: This quarter, revenue was $82.9 billion, up 1815% "
                    "in constant currency. Gross margin dollars increased 1613% "
                    "in constant currency. Operating income increased 2016% in "
                    "constant currency. Earnings per share increased 218% in "
                    "constant currency. Microsoft Cloud grew 2925% in constant "
                    "currency. Productivity revenue grew 1713% in constant "
                    "currency. Commercial cloud revenue increased 1915% in "
                    "constant currency. Consumer cloud revenue increased 3329% "
                    "in constant currency. Dynamics revenue increased 2217% in "
                    "constant currency. Cloud segment revenue grew 3028% in "
                    "constant currency. Azure grew 4039% in constant currency. "
                    "Operating expenses increased 98% in constant currency. "
                    "LinkedIn revenue increased 129% in constant currency. "
                    "Revenue increased slightly and decreased 3% in constant "
                    "currency. On a reported basis, we expect revenue growth to "
                    "be between 13 and 14% in constant currency. We expect growth "
                    "to be between 13% and 14% in constant currency. We expect "
                    "growth of 12% to 13% in constant currency."
                ),
                "Operator: We will now begin Q&A.",
                "Keith Weiss: Thank you.",
            ]
        )
    )

    text = parsed['prepared_remarks'][1]['text']
    assert 'up 18% and 15% in constant currency' in text
    assert 'increased 16% and 13% in constant currency' in text
    assert 'Earnings per share increased 21% and 18% in constant currency' in text
    assert 'Operating expenses increased 9% and 8% in constant currency' in text
    assert 'LinkedIn revenue increased 129% in constant currency' in text
    assert 'LinkedIn revenue increased 12% and 9% in constant currency' not in text
    assert 'decreased 3% in constant currency' in text
    assert 'between 13 and 14% in constant currency' in text
    assert 'between 13% and 14% in constant currency' in text
    assert '12% to 13% in constant currency' in text
    assert parsed['metadata']['text_repair_count'] == 12
    assert parsed['metadata']['text_repair_types'] == [
        'fused_constant_currency_percent_pair'
    ]

    body, _ = _build_transcript_body({
        **parsed,
        'symbol': 'MSFT',
        'quarter': 3,
        'year': 2026,
        'date': '2026-04-29',
    })
    assert (
        'Transcript text repairs: 12 FMP constant-currency percentage pair repairs applied.'
        in body
    )


def test_parse_transcript_does_not_split_isolated_constant_currency_percent() -> None:
    parsed = parse_transcript(
        "Amy Hood: Revenue grew 46% in constant currency on strong demand."
    )

    text = parsed['prepared_remarks'][0]['text']
    assert 'Revenue grew 46% in constant currency' in text
    assert 'text_repair_count' not in parsed['metadata']


def test_parse_transcript_does_not_split_two_digit_percent_on_light_systematic_signal() -> None:
    parsed = parse_transcript(
        (
            "Amy Hood: Revenue was up 1815% in constant currency. Gross margin "
            "dollars increased 1613% in constant currency. Operating income "
            "increased 2016% in constant currency. Azure grew 46% in constant "
            "currency."
        )
    )

    text = parsed['prepared_remarks'][0]['text']
    assert 'up 18% and 15% in constant currency' in text
    assert 'Azure grew 46% in constant currency' in text
    assert 'Azure grew 4% and 6% in constant currency' not in text
    assert parsed['metadata']['text_repair_count'] == 3


def test_parse_transcript_does_not_split_two_digit_percent_on_separate_speaker_line() -> None:
    parsed = parse_transcript(
        "\n".join(
            [
                (
                    "Amy Hood: Revenue was up 1815% in constant currency. Gross "
                    "margin dollars increased 1613% in constant currency. "
                    "Operating income increased 2016% in constant currency. "
                    "Microsoft Cloud grew 2925% in constant currency. Productivity "
                    "revenue grew 1713% in constant currency. Commercial cloud "
                    "revenue increased 1915% in constant currency. Consumer cloud "
                    "revenue increased 3329% in constant currency. Dynamics revenue "
                    "increased 2217% in constant currency."
                ),
                "Satya Nadella: Azure grew 46% in constant currency.",
            ]
        )
    )

    text = parsed['prepared_remarks'][1]['text']
    assert 'Azure grew 46% in constant currency' in text
    assert 'Azure grew 4% and 6% in constant currency' not in text


def test_parse_transcript_does_not_split_standalone_three_digit_growth_in_repair_mode() -> None:
    parsed = parse_transcript(
        (
            "Amy Hood: Revenue was up 1815% in constant currency. Gross margin "
            "dollars increased 1613% in constant currency. Operating income "
            "increased 2016% in constant currency. AI services grew 100% in "
            "constant currency. Security revenue grew 110% in constant currency. "
            "New product revenue grew 150% in constant currency."
        )
    )

    text = parsed['prepared_remarks'][0]['text']
    assert 'up 18% and 15% in constant currency' in text
    assert 'AI services grew 100% in constant currency' in text
    assert 'Security revenue grew 110% in constant currency' in text
    assert 'New product revenue grew 150% in constant currency' in text
    assert '10% and 0% in constant currency' not in text
    assert '11% and 10% in constant currency' not in text
    assert '15% and 0% in constant currency' not in text
    assert parsed['metadata']['text_repair_count'] == 3


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


def test_file_residency_detects_sf_dataless_without_opening(monkeypatch) -> None:
    dataless_flag = 0x40000000
    monkeypatch.setattr(file_residency.stat, 'SF_DATALESS', dataless_flag, raising=False)
    monkeypatch.setattr(
        file_residency.Path,
        'stat',
        lambda _path: SimpleNamespace(st_flags=dataless_flag | 0x20),
    )

    assert file_residency.is_dataless_file('/corpus/placeholder.md') is True


def test_get_earnings_transcript_refetches_dataless_cache_then_replaces_atomically(
    tmp_path,
    monkeypatch,
) -> None:
    cache_path = tmp_path / 'parsed.json'
    placeholder = 'non-resident-placeholder-must-not-be-opened'
    cache_path.write_text(placeholder, encoding='utf-8')
    events: list[str] = []

    class _Series:
        def __init__(self, value):
            self._value = value
            self.iloc = self

        def __getitem__(self, index):
            assert index == 0
            return self._value

    class _Frame:
        empty = False
        columns = {'content', 'date'}

        def __getitem__(self, key):
            if key == 'content':
                content = (
                    'Operator: Welcome to the earnings call.\n'
                    'Satya Nadella: ' + 'Cloud demand remained strong. ' * 30
                )
                return _Series(content)
            if key == 'date':
                return _Series('2025-01-29')
            raise KeyError(key)

    class _Client:
        def fetch(self, endpoint, **kwargs):
            assert endpoint == 'earnings_transcript'
            assert kwargs == {'symbol': 'MSFT', 'year': 2025, 'quarter': 1}
            assert cache_path.read_text(encoding='utf-8') == placeholder
            events.append('fetch')
            return _Frame()

    original_atomic_write = transcripts.atomic_write_text

    def record_atomic_write(path, content):
        assert events == ['fetch']
        events.append('atomic_write')
        original_atomic_write(path, content)

    monkeypatch.setattr(transcripts, '_get_cache_path', lambda *_args: cache_path)
    monkeypatch.setattr(transcripts, 'is_dataless_file', lambda path: path == cache_path)
    monkeypatch.setattr(transcripts, 'FMPClient', _Client)
    monkeypatch.setattr(transcripts, 'atomic_write_text', record_atomic_write)

    response = transcripts.get_earnings_transcript(
        symbol='MSFT',
        year=2025,
        quarter=1,
        section='prepared_remarks',
        format='full',
    )

    assert response['status'] == 'success'
    assert events == ['fetch', 'atomic_write']
    cached = json.loads(cache_path.read_text(encoding='utf-8'))
    assert cached['symbol'] == 'MSFT'
    assert cached['year'] == 2025
    assert cached['quarter'] == 1


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
