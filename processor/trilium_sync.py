"""Sync papers and concepts to Trilium Notes knowledge graph via ETAPI."""

import logging

import httpx

logger = logging.getLogger(__name__)


class TriliumSync:
    def __init__(self, trilium_url: str, etapi_token: str):
        self.base_url = f"{trilium_url}/etapi"
        self.headers = {
            "Authorization": etapi_token,
            "Content-Type": "application/json",
        }
        self._http = httpx.Client(timeout=15.0, headers=self.headers)
        self._concept_cache: dict[str, str] = {}
        self._root_note_ids: dict[str, str] = {}

    def _ensure_root_structure(self):
        """Create root folder structure if it doesn't exist."""
        if self._root_note_ids:
            return

        root = self._find_or_create_note("root", "Research Vault", "book")
        self._root_note_ids["vault"] = root
        self._root_note_ids["concepts"] = self._find_or_create_note(
            root, "Concepts", "book"
        )
        self._root_note_ids["papers"] = self._find_or_create_note(
            root, "Papers", "book"
        )
        self._root_note_ids["papers_arxiv"] = self._find_or_create_note(
            self._root_note_ids["papers"], "arxiv", "book"
        )
        self._root_note_ids["papers_ssrn"] = self._find_or_create_note(
            self._root_note_ids["papers"], "SSRN", "book"
        )
        self._root_note_ids["papers_internal"] = self._find_or_create_note(
            self._root_note_ids["papers"], "Internal Research", "book"
        )
        self._root_note_ids["papers_other"] = self._find_or_create_note(
            self._root_note_ids["papers"], "Other Sources", "book"
        )
        self._root_note_ids["instruments"] = self._find_or_create_note(
            root, "Instruments", "book"
        )
        self._root_note_ids["my_notes"] = self._find_or_create_note(
            root, "My Notes", "book"
        )

    def _find_or_create_note(
        self, parent_id: str, title: str, note_type: str = "text"
    ) -> str:
        """Find existing child note by title or create it."""
        # Search for existing note with this title under the parent
        search_query = f'note.title = "{title}"'
        if parent_id != "root":
            search_query += f' AND note.parents.noteId = "{parent_id}"'
        resp = self._http.get(
            f"{self.base_url}/notes",
            params={"search": search_query},
        )
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                return results[0]["noteId"]

        # Create new note
        resp = self._http.post(
            f"{self.base_url}/create-note",
            json={
                "parentNoteId": parent_id,
                "title": title,
                "type": note_type,
                "content": "",
            },
        )
        resp.raise_for_status()
        note_id = resp.json()["note"]["noteId"]
        logger.info(f"Created Trilium note: {title} ({note_id})")
        return note_id

    def create_paper_note(
        self,
        title: str,
        source: str,
        url: str,
        authors: list[str],
        abstract: str,
        published_date: str,
        concepts: list[str],
        instruments: list[str],
        methodology: str = "",
        relevance_score: int = 0,
    ) -> str:
        """Create a paper note in Trilium and link it to concepts/instruments."""
        self._ensure_root_structure()

        # Determine parent folder
        source_folder_map = {
            "arxiv": "papers_arxiv",
            "ssrn": "papers_ssrn",
            "internal": "papers_internal",
        }
        parent_key = source_folder_map.get(source, "papers_other")
        parent_id = self._root_note_ids[parent_key]

        # Build note content
        stars = relevance_score * "*"
        no_stars = (5 - relevance_score) * "."
        content = f"""<h2>{title}</h2>
<p><strong>Authors:</strong> {', '.join(authors)}</p>
<p><strong>Date:</strong> {published_date}</p>
<p><strong>Source:</strong> {source} | <a href="{url}">Link</a></p>
<p><strong>Methodology:</strong> {methodology}</p>
<p><strong>Relevance:</strong> [{stars}{no_stars}]</p>
<p><strong>Instruments:</strong> {', '.join(instruments) if instruments else 'General'}</p>
<p><strong>Concepts:</strong> {', '.join(concepts) if concepts else 'Untagged'}</p>
<hr>
<h3>Abstract</h3>
<p>{abstract}</p>"""

        # Create the paper note
        resp = self._http.post(
            f"{self.base_url}/create-note",
            json={
                "parentNoteId": parent_id,
                "title": title[:200],
                "type": "text",
                "content": content,
            },
        )
        resp.raise_for_status()
        paper_note_id = resp.json()["note"]["noteId"]

        # Add labels
        self._add_label(paper_note_id, "source", source)
        self._add_label(paper_note_id, "published_date", published_date)
        self._add_label(paper_note_id, "methodology", methodology)
        self._add_label(paper_note_id, "relevance", str(relevance_score))

        # Link to concept notes
        for concept in concepts:
            concept_id = self.get_or_create_concept(concept)
            self._create_relation(paper_note_id, "references", concept_id)

        # Link to instrument notes
        for instrument in instruments:
            instr_id = self._get_or_create_instrument(instrument)
            self._create_relation(paper_note_id, "studies", instr_id)

        logger.info(f"Created paper note: {title[:60]} ({paper_note_id})")
        return paper_note_id

    def get_or_create_concept(self, concept_name: str) -> str:
        """Get or create a concept note. Cached."""
        normalized = concept_name.strip().title()
        if normalized in self._concept_cache:
            return self._concept_cache[normalized]

        self._ensure_root_structure()
        note_id = self._find_or_create_note(
            self._root_note_ids["concepts"], normalized
        )
        self._concept_cache[normalized] = note_id
        return note_id

    def _get_or_create_instrument(self, symbol: str) -> str:
        """Get or create an instrument note."""
        self._ensure_root_structure()
        return self._find_or_create_note(
            self._root_note_ids["instruments"], symbol.upper()
        )

    def _add_label(self, note_id: str, name: str, value: str):
        """Add a label attribute to a note."""
        try:
            self._http.post(
                f"{self.base_url}/attributes",
                json={"noteId": note_id, "type": "label", "name": name, "value": value},
            )
        except Exception as e:
            logger.warning(f"Failed to add label {name}={value}: {e}")

    def _create_relation(self, source_id: str, relation_name: str, target_id: str):
        """Create a relation between two notes."""
        try:
            self._http.post(
                f"{self.base_url}/attributes",
                json={"noteId": source_id, "type": "relation", "name": relation_name, "value": target_id},
            )
        except Exception as e:
            logger.warning(f"Failed to create relation {relation_name}: {e}")

    def close(self):
        self._http.close()
