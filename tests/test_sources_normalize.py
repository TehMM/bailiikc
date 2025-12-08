from app.scraper import sources


def test_normalize_source_defaults_and_aliases() -> None:
    assert sources.normalize_source(None) == sources.UNREPORTED_JUDGMENTS
    assert sources.normalize_source("") == sources.UNREPORTED_JUDGMENTS
    assert sources.normalize_source("unreported_judgments") == sources.UNREPORTED_JUDGMENTS
    assert sources.normalize_source("uj") == sources.UNREPORTED_JUDGMENTS
    assert sources.normalize_source("unreported") == sources.UNREPORTED_JUDGMENTS
    assert sources.normalize_source("public-registers") == sources.PUBLIC_REGISTERS
    assert sources.normalize_source("pr") == sources.PUBLIC_REGISTERS
    assert sources.normalize_source("unknown") == sources.UNREPORTED_JUDGMENTS
