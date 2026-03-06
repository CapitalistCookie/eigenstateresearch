"""Sync internal research docs from git repos on GCE VM.

Since the GCE VM doesn't have direct access to the local VM's filesystem,
internal docs are synced by cloning/pulling git repos and scanning
research/docs/ and docs/plans/ directories.
"""

import logging
import os
import subprocess

from base import BaseScraper
from config import settings
from shared.models import RawDocument

logger = logging.getLogger(__name__)


class InternalWatcher(BaseScraper):
    def __init__(self, redis_url: str):
        super().__init__(redis_url, "internal")

    def process_file(self, filepath: str) -> bool:
        """Process a single internal markdown file."""
        try:
            with open(filepath, "r") as f:
                content = f.read()

            filename = os.path.basename(filepath)
            # Extract date from filename if it follows our convention
            date_str = filename[:10] if len(filename) > 10 and filename[4] == "-" else ""

            doc = RawDocument(
                source="internal",
                url=f"file://{filepath}",
                title=filename.replace(".md", "").replace("_", " "),
                authors=["Eigenstate Research"],
                abstract=content[:2000],
                published_date=date_str,
                html_content=content,
                tags=["internal"],
                metadata={"filepath": filepath},
            )

            return self.submit(doc)
        except Exception as e:
            logger.error(f"Failed to process {filepath}: {e}")
            return False

    def _git_sync(self):
        """Clone or pull all configured git repos. Skips repos that fail (e.g. private repos without creds)."""
        base_dir = settings.git_repo_dir
        os.makedirs(base_dir, exist_ok=True)

        for repo_url in settings.git_repos:
            repo_name = repo_url.split("/")[-1].replace(".git", "")
            repo_dir = os.path.join(base_dir, repo_name)

            try:
                if not os.path.exists(repo_dir):
                    logger.info(f"Cloning {repo_url} to {repo_dir}...")
                    subprocess.run(
                        ["git", "clone", "--depth=1", "--sparse", repo_url, repo_dir],
                        check=True,
                    )
                    subprocess.run(
                        ["git", "sparse-checkout", "set", "research/docs", "docs/plans", "docs"],
                        cwd=repo_dir, check=True,
                    )
                else:
                    subprocess.run(["git", "pull", "--ff-only"], cwd=repo_dir, check=True)
            except subprocess.CalledProcessError as e:
                logger.warning(f"Git sync failed for {repo_url}: {e}. Skipping.")

    def run(self) -> int:
        """Pull latest git repos and scan internal docs."""
        self._git_sync()
        count = 0

        for repo_url in settings.git_repos:
            repo_name = repo_url.split("/")[-1].replace(".git", "")
            repo_dir = os.path.join(settings.git_repo_dir, repo_name)

            for subdir in ["research/docs", "docs/plans", "docs"]:
                dir_path = os.path.join(repo_dir, subdir)
                if not os.path.exists(dir_path):
                    continue
                for filename in os.listdir(dir_path):
                    if filename.endswith(".md"):
                        if self.process_file(os.path.join(dir_path, filename)):
                            count += 1

        logger.info(f"Internal scan: {count} new documents")
        return count
