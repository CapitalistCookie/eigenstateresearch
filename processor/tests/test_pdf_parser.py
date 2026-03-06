from pdf_parser import parse_pdf_bytes, parse_html


def test_parse_html():
    html = """
    <html><body>
    <h1>Market Microstructure</h1>
    <p>This paper studies order flow dynamics in futures markets.</p>
    <p>We find that cumulative delta predicts short-term direction.</p>
    </body></html>
    """
    text = parse_html(html)
    assert "order flow dynamics" in text
    assert "cumulative delta" in text
    assert "<html>" not in text  # No raw HTML tags


def test_parse_pdf_bytes_returns_text():
    # Create a minimal PDF in memory using pymupdf
    import pymupdf

    doc = pymupdf.open()
    page = doc.new_page()
    page.insert_text((72, 72), "Test paper about algorithmic trading.")
    pdf_bytes = doc.tobytes()
    doc.close()

    text = parse_pdf_bytes(pdf_bytes)
    assert "algorithmic trading" in text
    assert len(text) > 10
