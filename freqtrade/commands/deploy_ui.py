import logging
import os
from pathlib import Path

import requests


logger = logging.getLogger(__name__)

# Timeout for requests
req_timeout = 30
_DEFAULT_UI_REPO = "https://api.github.com/repos/freqtrade/frequi/"


def clean_ui_subdir(directory: Path):
    if directory.is_dir():
        logger.info("Removing UI directory content.")

        for p in reversed(list(directory.glob("**/*"))):  # iterate contents from leaves to root
            if p.name in (".gitkeep", "fallback_file.html"):
                continue
            if p.is_file():
                p.unlink()
            elif p.is_dir():
                p.rmdir()


def read_ui_version(dest_folder: Path) -> str | None:
    file = dest_folder / ".uiversion"
    if not file.is_file():
        return None

    with file.open("r") as f:
        return f.read()


def download_and_install_ui(dest_folder: Path, dl_url: str, version: str, strip_components: int = 0):
    from io import BytesIO
    from zipfile import ZipFile

    logger.info(f"Downloading {dl_url}")
    resp = requests.get(dl_url, timeout=req_timeout).content
    dest_folder.mkdir(parents=True, exist_ok=True)
    with ZipFile(BytesIO(resp)) as zf:
        for fn in zf.filelist:
            with zf.open(fn) as x:
                parts = Path(fn.filename).parts
                if strip_components:
                    parts = parts[strip_components:]
                if not parts:
                    continue
                destfile = dest_folder.joinpath(*parts)
                if fn.is_dir():
                    destfile.mkdir(exist_ok=True)
                else:
                    destfile.write_bytes(x.read())
    with (dest_folder / ".uiversion").open("w") as f:
        f.write(version)


def _get_ui_repo_url() -> str:
    """
    Return the API base url for the UI repository.
    Allows overriding via FREQTRADE_UI_REPO env variable.
    """
    base_url = os.getenv("FREQTRADE_UI_REPO", _DEFAULT_UI_REPO)
    if not base_url.endswith("/"):
        base_url = f"{base_url}/"
    return base_url


def _fallback_branch_download(base_url: str) -> tuple[str, str, int]:
    """
    Fallback for forks without releases:
    download archive from default branch and use short commit hash as version.
    """
    repo_url = base_url.rstrip("/")
    repo_resp = requests.get(repo_url, timeout=req_timeout)
    repo_resp.raise_for_status()
    repo_info = repo_resp.json()
    branch = repo_info.get("default_branch", "main")

    commit_resp = requests.get(f"{base_url}commits/{branch}", timeout=req_timeout)
    commit_resp.raise_for_status()
    commit = commit_resp.json()
    commit_sha = commit.get("sha", branch)
    version = f"{branch}-{commit_sha[:7]}" if commit_sha else branch
    dl_url = f"{base_url}zipball/{branch}"
    logger.info(
        "Installing freqUI from %s branch '%s' (commit %s).",
        repo_info.get("full_name"),
        branch,
        commit_sha[:7] if commit_sha else "unknown",
    )
    return dl_url, version, 1


def get_ui_download_url(version: str | None, prerelease: bool) -> tuple[str, str, int]:
    base_url = _get_ui_repo_url()
    # Get base UI Repo path

    releases = []
    try:
        resp = requests.get(f"{base_url}releases", timeout=req_timeout)
        resp.raise_for_status()
        releases = resp.json()
    except requests.HTTPError as exc:
        logger.warning("Unable to fetch UI releases from %s (%s).", base_url, exc)

    tmp = []
    if version:
        tmp = [x for x in releases if x["name"] == version]
    else:
        tmp = [x for x in releases if prerelease or not x.get("prerelease")]

    if tmp:
        # Ensure we have the latest version
        if version is None:
            tmp.sort(key=lambda x: x["created_at"], reverse=True)
        latest_version = tmp[0]["name"]
        assets = tmp[0].get("assets", [])
    elif version:
        raise ValueError("UI-Version not found.")
    else:
        logger.warning(
            "No releases available for %s. Falling back to default branch archive.",
            base_url,
        )
        return _fallback_branch_download(base_url)

    dl_url = ""
    if assets and len(assets) > 0:
        dl_url = assets[0]["browser_download_url"]

    # URL not found - try assets url
    if not dl_url:
        assets_url = releases[0].get("assets_url") if releases else None
        if assets_url:
            resp = requests.get(assets_url, timeout=req_timeout)
            resp.raise_for_status()
            asset_entries = resp.json()
            if asset_entries:
                dl_url = asset_entries[0]["browser_download_url"]

    if not dl_url:
        logger.warning("Release assets missing for %s. Falling back to latest branch archive.", base_url)
        return _fallback_branch_download(base_url)

    return dl_url, latest_version, 0
