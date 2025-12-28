#!/usr/bin/env python3
import os.path
import itertools
import subprocess
import json
import logging
from io import BytesIO
import sys
import os
import functools
import re
import sys
from itertools import chain
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse

# from tempfile import gettempdir
import datetime
import csv
import hashlib


import requests
import yaml
import pywikibot
from pywikibot import Site, Page, FilePage
from zhconv_rs import zhconv as zhconv_


# CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yml")
POSITION_FILE_PATH = os.path.join(os.path.dirname(__file__), ".position")
BLOB_DIR = Path(__file__).parent / "blobs"
CACHE_FILE_PATH = Path(__file__).parent / ".cache.pdf"
RETRY_TIMES = 3
CHUNK_SIZE = 4 * 1024 * 1024
# TEMP_DIR = Path()gettempdir())

USER_AGENT = "zjlibpdbot/0.0 (+https://github.com/gowee/zjlibpd)"

# RESP_DUMP_PATH = "/tmp/wmc_upload_resp_dump.html"

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
logger = logging.getLogger(__name__)


def call(command, *args, **kwargs):
    kwargs["shell"] = True
    return subprocess.check_call(command, *args, **kwargs)


def load_position(name):
    logger.info(f'Loading position from {POSITION_FILE_PATH + "." + name}')
    if os.path.exists(POSITION_FILE_PATH + "." + name):
        with open(POSITION_FILE_PATH + "." + name, "r") as f:
            return f.read().strip()
    else:
        return None


def store_position(name, position):
    with open(POSITION_FILE_PATH + "." + name, "w") as f:
        f.write(position)


def retry(times=RETRY_TIMES):
    def wrapper(fn):
        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            tried = 0
            while True:
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    tried += 1
                    if tried > times:
                        # raise Exception(f"Failed finally after {times} tries") from e
                        raise e
                    logger.warning(f"Retrying {fn} {tried}/{times}", exc_info=e)

        return wrapped

    return wrapper


# https://stackoverflow.com/a/2506477
def rproxy_url(url, rproxy=None):
    if rproxy is None:
        return url
    parts = list(urlparse(rproxy))
    q = dict(parse_qsl(parts[4]))
    q.update({"url": url})
    parts[4] = urlencode(q)
    return urlunparse(parts)


@retry(3)
def fetch_file(url, session=None, rproxy=None):
    url = rproxy_url(url, rproxy)
    # prefetched = f"../../public_html/wzlib/{sha1sum(url)}.pdf"
    # if os.path.exists(prefetched):
    #     logger.info(f"Hit prefetched: {prefetched}")
    #     return prefetched
    # else:
    logger.info(f"Downloading {url}")
    get = session and session.get or requests.get
    headers = {"User-Agent": USER_AGENT}
    resp = get(
        url,
        headers=headers,
    )
    resp.raise_for_status()
    assert len(resp.content) != 0, "Got empty file"
    if "Content-Length" in resp.headers:
        # https://blog.petrzemek.net/2018/04/22/on-incomplete-http-reads-and-the-requests-library-in-python/
        expected_size = int(resp.headers["Content-Length"])
        actual_size = resp.raw.tell()
        assert (
            expected_size == actual_size
        ), f"Incomplete download: {actual_size}/{expected_size}"
    with open(CACHE_FILE_PATH, "wb") as f:
        f.write(resp.content)
    return CACHE_FILE_PATH


def sha1sum(s):
    h = hashlib.sha1()
    h.update(s.encode("utf-8"))
    return h.hexdigest()


def get_page_creator_id(page):
    creator_id = None
    try:
        creator_id = next(page.revisions(True, 1))["userid"]
    except (StopIteration, pywikibot.exceptions.NoPageError):
        pass
    return creator_id


def is_wikitext_modified_by_others(page, ours=None, check_limit=100):
    for rev in page.revisions(check_limit):
        if rev["user"] not in ours and rev["userid"] not in ours:
            return rev
    return None


def is_fresh(page):
    try:
        timestamp = next(page.revisions(True, 1))["timestamp"]
    except (StopIteration, pywikibot.exceptions.NoPageError):
        return True
    return pywikibot.Timestamp.utcnow() - timestamp < datetime.timedelta(days=15)


def generate_redirection(wikitext, dest):
    return f"#REDIRECT [[{dest}]]\n\n<!--\n" + wikitext + "\n-->"


def main():
    csv.field_size_limit(sys.maxsize)

    assert (
        3 <= len(sys.argv) <= 4
    ), "Usage: python3 upload.py <task_tag> <tsv> [--no-update/--force-update]"
    tag = sys.argv[1]
    update = "auto"

    if len(sys.argv) == 4:
        if sys.argv[3] == "--no-update":
            update = "no"
        elif sys.argv[3] == "--force-update":
            update = "force"
        else:
            raise ValueError(f"Unknown option: {sys.argv[3]}")

    if rproxy := os.environ.get("RPROXY"):
        logger.info("RPROXY: " + rproxy)
    if copyupload := os.environ.get("COPYUPLOAD"):
        logger.info("CopyUpload activated")

    # with open(CONFIG_FILE_PATH, "r") as f:
    #     config = yaml.safe_load(f.read())

    # username, password = config["username"], config["password"]
    site = Site("commons")
    site.login()
    # site.login(username, password)
    # site.requests["timeout"] = 125
    # site.chunk_size = 1024 * 1024 * 64

    # logger.info(f"Signed in as {username}")
    logger.info("Up")

    username = site.userinfo["name"]

    logpagename = f"User:{username}/logs/{tag}"
    logpage = pywikibot.Page(site, logpagename)
    if not logpage.exists():
        logger.warning(f"{logpagename} is not created yet")

    def log(l, local_level=logging.INFO, exc_info=None):
        """Log to the local and the remote at the same time"""
        logger.log(level=local_level, msg=l, exc_info=exc_info)

        l = re.sub(
            r"[-a-zA-Z0-9.]+\.(trycloudflare\.com|workers\.dev|toolforge\.org)", "", l
        )  # mask RPROXY domain
        d = str(datetime.datetime.now(datetime.timezone.utc))
        wikitext = ""
        wikitext = logpage.text
        wikitext += f"\n* <code>{d} - {tag}</code> " + l + "\n"
        logger.debug(f"Log remote: {l}")
        logpage.text = wikitext
        logpage.save(f"Log ({tag}): {l}")

    last_position = load_position(tag)

    failcnt = 0

    with open(sys.argv[2]) as f:
        r = csv.reader(f, delimiter="\t")

        for pagename, wikitext, summary, file_url in r:
            if last_position is not None:
                if last_position == pagename:
                    logger.info(f"Last processed: {last_position}")
                    last_position = None
                continue
            if pagename.startswith("File:"):
                page = FilePage(site, pagename)
                # in some rare cases, the file is uploaded without creating the page
                page_existing = page.exists()
                file_existing = False
                try:
                    page.get_file_url()
                    file_existing = True
                except pywikibot.exceptions.PageRelatedError:
                    pass
            else:
                page = Page(site, pagename)
                page_existing = page.exists()
            if (
                pagename.startswith("File:")
                and not page.isRedirectPage()
                and not file_existing
            ):
                assert file_url
                try:
                    if copyupload:
                        proxied_url = rproxy_url(file_url, rproxy)
                        target = {"source_url": proxied_url}
                        logger.info(f"Copyuploading {pagename} from {proxied_url}")
                    else:
                        logger.info(f"Fetching {pagename}")
                        binary = fetch_file(file_url, rproxy=rproxy)
                        target = {"source_filename": binary}
                        logger.info(f"Uploading {pagename}")

                    @retry()
                    def do1():
                        try:
                            r = site.upload(
                                **target,
                                filepage=page,
                                text=wikitext,
                                comment=summary,
                                asynchronous=True,
                                chunk_size=CHUNK_SIZE,
                                ignore_warnings=False,
                            )
                        except pywikibot.exceptions.UploadError as e:
                            if e.code == "duplicate":
                                # dup = e.msg.removeprefix("File:")
                                dup = re.search(r"\['(.+)'\]", str(e)).group(1)
                                dup = dup.replace("_", " ")
                                if (
                                    dup.split(" ")[0]
                                    == pagename.removeprefix("File:").split(" ")[0]
                                    and get_page_creator_id(page) == site.userinfo["id"]
                                ):
                                    log(f"Moving [[:File:{dup}]] to [[:{pagename}]]")
                                    Page(site, "File:" + dup).move(
                                        pagename, reason="Update title", noredirect=True
                                    )
                                else:
                                    log(f"Dup: [[:{pagename}]] -> [[:{'File:' + dup}]]")
                                    page.text = generate_redirection(
                                        wikitext, "File:" + dup
                                    )
                                    page.save(
                                        summary + f" (Redirecting to [[File:{dup}]])"
                                    )
                                if not copyupload:
                                    os.remove(binary)
                            elif e.code == "exists":
                                log(
                                    "File exists. Conflicting with another instance?",
                                    local_level=logging.WARNING,
                                    exc_info=e,
                                )
                                if not copyupload:
                                    os.remove(binary)
                            else:
                                raise e
                        else:
                            assert r, f"Upload failed: {r}"
                            if not copyupload:
                                os.remove(binary)

                    do1()
                except Exception as e:
                    log(
                        f"Failed to upload [[:{pagename}]]: {e}",
                        local_level=logging.WARNING,
                        exc_info=e,
                    )
            else:

                @retry()
                def do2():
                    if page_existing and update == "no":
                        return
                    if update == "force" or (
                        (
                            not (creator_id := get_page_creator_id(page))
                            or creator_id == site.userinfo["id"]
                        )
                        and is_fresh(page)
                    ):
                        if pagename.startswith("File:") and page.isRedirectPage():
                            page.text = generate_redirection(
                                wikitext, page.getRedirectTarget().title()
                            )
                        else:
                            page.text = wikitext
                        if page_existing and (
                            rev := is_wikitext_modified_by_others(
                                page, ours=(site.userinfo["id"], "SchlurcherBot")
                            )
                        ):
                            log(
                                f"Updating [[:{pagename}]] that has been been modified by {rev['user']} at {rev['timestamp'].isoformat()}",
                                logging.WARNING,
                            )
                        page.save(summary + (" (Updating)" if page_existing else ""))
                    else:
                        log(
                            f"Refused to update page (stale or not-owning): [[:{pagename}]]"
                        )

                do2()
            store_position(tag, pagename)
    # logger.info(f"Batch done with {failcnt} failures.")


if __name__ == "__main__":
    main()
