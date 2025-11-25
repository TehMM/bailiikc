from app.scraper import run


def test_normalize_scrape_mode_variants():
    assert run._normalize_scrape_mode("resume") == "resume"
    assert run._normalize_scrape_mode(" ReSuMe ") == "resume"
    assert run._normalize_scrape_mode("weird") == "new"
