import os

import pytest

from trilium_sync import TriliumSync

TRILIUM_URL = os.environ.get("TRILIUM_URL", "http://localhost:8080")
TRILIUM_TOKEN = os.environ.get("TRILIUM_ETAPI_TOKEN", "")


@pytest.mark.skipif(not TRILIUM_TOKEN, reason="No Trilium ETAPI token")
class TestTriliumSync:
    @pytest.fixture
    def sync(self):
        return TriliumSync(trilium_url=TRILIUM_URL, etapi_token=TRILIUM_TOKEN)

    def test_create_paper_note(self, sync):
        note_id = sync.create_paper_note(
            title="Test Paper: Order Flow Analysis",
            source="arxiv",
            url="https://arxiv.org/abs/test123",
            authors=["Smith, J.", "Doe, A."],
            abstract="We study order flow in ES futures.",
            published_date="2024-01-15",
            concepts=["order flow", "market microstructure"],
            instruments=["ES"],
            methodology="empirical",
            relevance_score=4,
        )
        assert note_id is not None
        assert isinstance(note_id, str)

    def test_create_concept_note(self, sync):
        note_id = sync.get_or_create_concept("Order Flow")
        assert note_id is not None
        # Calling again should return the same note
        note_id2 = sync.get_or_create_concept("Order Flow")
        assert note_id == note_id2
