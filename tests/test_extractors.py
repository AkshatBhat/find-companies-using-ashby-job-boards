from ashby_discovery.extractors import extract_slug, extract_slugs_from_html, has_embed_marker


def test_extract_slug_from_url() -> None:
    assert extract_slug("https://jobs.ashbyhq.com/acme") == "acme"
    assert extract_slug("https://jobs.ashbyhq.com/acme/embed?version=2") == "acme"
    assert extract_slug("https://jobs.ashbyhq.com/Checkbox%20Technology") == "Checkbox Technology"


def test_extract_slug_from_token() -> None:
    assert extract_slug("acme_inc") == "acme_inc"
    assert extract_slug("bad slug") == "bad slug"


def test_extract_slugs_from_html_embed_markers() -> None:
    html = """
    <html>
      <script>
        window.__ashbyBaseJobBoardUrl = 'https://jobs.ashbyhq.com/rocketco';
      </script>
      <iframe src=\"https://jobs.ashbyhq.com/moonshot/embed?version=2\"></iframe>
    </html>
    """
    slugs = extract_slugs_from_html(html)
    assert "rocketco" in slugs
    assert "moonshot" in slugs
    assert has_embed_marker(html)
