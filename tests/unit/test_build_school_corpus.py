import json

import pytest

from scripts.build_school_corpus import RedactionError, build_school_corpus, redact_document, validate_redaction


def test_redact_document_replaces_sensitive_values_and_reuses_placeholders():
    text = (
        "Contact christine.jinae@gmail.com or christine.jinae@gmail.com.\n"
        "Visit https://forms.gle/example and https://forms.gle/example.\n"
        "Call 416-555-1212.\n"
        "Frankland Community School code is Classroom92.\n"
        "Shelley Lue-Kim and Nolan will attend Room 106."
    )
    metadata = {
        "school_names": ["Frankland Community School"],
        "staff_names": ["Shelley Lue-Kim"],
        "child_names": ["Nolan"],
    }

    redacted, report = redact_document(text, metadata=metadata, source_filename="sample.txt")

    assert "christine.jinae@gmail.com" not in redacted
    assert "https://forms.gle/example" not in redacted
    assert "416-555-1212" not in redacted
    assert "Frankland Community School" not in redacted
    assert "Shelley Lue-Kim" not in redacted
    assert "Nolan" not in redacted
    assert redacted.count("EMAIL_1") == 2
    assert redacted.count("REGISTRATION_LINK_1") == 2
    assert "PHONE_1" in redacted
    assert "ACCESS_CODE_1" in redacted
    assert "SCHOOL_NAME" in redacted
    assert "STAFF_NAME_1" in redacted
    assert "CHILD_NAME_1" in redacted
    assert "CLASSROOM_ID_1" in redacted
    assert report.placeholder_counts["EMAIL"] == 1
    assert report.placeholder_counts["REGISTRATION_LINK"] == 1


def test_redact_document_resets_placeholder_numbering_per_document():
    metadata = {}
    first, _ = redact_document("Email one: first@example.com", metadata=metadata, source_filename="one.txt")
    second, _ = redact_document("Email two: second@example.com", metadata=metadata, source_filename="two.txt")

    assert "EMAIL_1" in first
    assert "EMAIL_1" in second


def test_validate_redaction_fails_when_known_school_name_remains():
    with pytest.raises(RedactionError):
        validate_redaction(
            "Frankland Community School families are invited.",
            metadata={"school_names": ["Frankland Community School"]},
        )


def test_build_school_corpus_imports_manifest_and_generates_eval(tmp_path):
    raw_path = tmp_path / "newsletter.txt"
    raw_path.write_text(
        "Frankland Community School progress reports go home on Wednesday, November 12. "
        "Use classroom92 to book interviews at https://parentinterview.example.com.",
        encoding="utf-8",
    )
    manifest = {
        "documents": {
            "newsletter.txt": {
                "entry_id": "reports-seed",
                "doc_type": "newsletter",
                "scope": "school_wide",
                "topics": ["academic reporting", "parent action items"],
                "event_types": ["reporting deadline"],
                "commonness": "routine",
                "action_required": "deadline",
                "audience": "all families",
                "salient_phrases": ["progress reports", "book interviews"],
                "source_kind": "email_body",
                "school_names": ["Frankland Community School"],
                "access_codes": ["classroom92"],
                "summary_weight": "important"
            }
        }
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    corpus, eval_payload, qa_payload = build_school_corpus(tmp_path)

    assert corpus["entries"][0]["entry_id"] == "reports-seed"
    assert corpus["entries"][0]["redacted_body"].count("SCHOOL_NAME") == 1
    assert "ACCESS_CODE_1" in corpus["entries"][0]["redacted_body"]
    assert "REGISTRATION_LINK_1" in corpus["entries"][0]["redacted_body"]
    assert eval_payload["items"][0]["expected_topics"] == ["academic reporting", "parent action items"]
    assert eval_payload["items"][0]["summary_weight"] == "important"
    assert qa_payload["items"][0]["entry_id"] == "reports-seed"
