"""is_bot_blocked_log must survive changes to the hint text after yt-dlp's message."""

import pytest

from distributed_core import is_bot_blocked_log

VIDEO = "dQw4w9WgXcQ"
CURRENT = (
    f"ERROR: [youtube] {VIDEO}: Sign in to confirm you're not a bot. "
    "Use --cookies-from-browser or --cookies for the authentication. "
    "See  https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp  "
    "for how to manually pass cookies. Also see  "
    "https://github.com/yt-dlp/yt-dlp/wiki/Extractors#exporting-youtube-cookies  "
    "for tips on effectively exporting YouTube cookies"
)


@pytest.mark.parametrize(
    "log",
    [
        CURRENT,
        f"ERROR: [youtube] {VIDEO}: Sign in to confirm you're not a bot. This helps protect our community.",
        f"ERROR: [youtube] {VIDEO}: Sign in to confirm you’re not a bot.",
        f"ERROR: [youtube] {VIDEO}: Sign in to confirm youre not a bot",
        f"[youtube] Extracting URL\n\x1b[0;31mERROR:\x1b[0m [youtube] {VIDEO}: Sign in to confirm you're not a bot.\n\n",
    ],
)
def test_detects_bot_check(log):
    assert is_bot_blocked_log(log)


@pytest.mark.parametrize(
    "log",
    [
        "",
        None,
        f"ERROR: [youtube] {VIDEO}: Video unavailable",
        f"ERROR: [youtube] {VIDEO}: Sign in to confirm your age.",
        # The bot check earlier in the log is not what ended the download.
        f"ERROR: [youtube] {VIDEO}: Sign in to confirm you're not a bot.\nERROR: [youtube] {VIDEO}: Private video",
    ],
)
def test_ignores_other_errors(log):
    assert not is_bot_blocked_log(log)


def test_reads_log_file(tmp_path):
    path = tmp_path / f"{VIDEO}.log"
    path.write_text(CURRENT + "\n", encoding="utf-8")
    assert is_bot_blocked_log(str(path))
