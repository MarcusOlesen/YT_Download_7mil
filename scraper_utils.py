import importlib.util
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from importlib import metadata as importlib_metadata
from pathlib import Path


PROFILE_FILENAME = "worker_profile.json"
DEFAULT_COOKIE_RELATIVE_PATH = os.path.join("auth", "youtube_cookies.txt")
DEFAULT_BGUTIL_SERVER_HOME = os.path.expanduser(r"~\bgutil-ytdlp-pot-provider\server")
GLOBAL_COOLDOWN_STATUS_INTERVAL_SECONDS = 60

PRESET_CONFIGS = {
    "conservative": {
        "target_titles_per_hour": 900,
        "jitter_min_seconds": 0.10,
        "jitter_max_seconds": 0.40,
        "rate_limit_cooldown_seconds": 5400,
        "max_rate_limit_retries_per_url": 1,
        "token_ttl_hours": 6,
    },
    "normal": {
        "target_titles_per_hour": 1300,
        "jitter_min_seconds": 0.05,
        "jitter_max_seconds": 0.25,
        "rate_limit_cooldown_seconds": 3600,
        "max_rate_limit_retries_per_url": 2,
        "token_ttl_hours": 6,
    },
    "fast": {
        "target_titles_per_hour": 1700,
        "jitter_min_seconds": 0.00,
        "jitter_max_seconds": 0.20,
        "rate_limit_cooldown_seconds": 2400,
        "max_rate_limit_retries_per_url": 2,
        "token_ttl_hours": 6,
    },
}

PERMANENT_MARKERS = [
    "private video",
    "video unavailable",
    "this video is unavailable",
    "has been removed",
    "account associated with this video has been terminated",
]

FORMAT_MARKERS = [
    "requested format is not available",
    "no video formats found",
    "no suitable format",
]

RATE_LIMIT_MARKERS = [
    "rate-limited by youtube",
    "too many requests",
    "try again later",
    "http error 429",
]


_pipeline_lock = threading.Lock()
_pipeline_state = {
    "configured": False,
    "run_dir": "",
    "profile": None,
    "cooldown_reader": None,
    "cooldown_writer": None,
    "next_request_not_before": 0.0,
    "local_cooldown_until": 0.0,
    "last_cooldown_report_at": 0.0,
    "request_pacing_lock": threading.Lock(),
    "cooldown_lock": threading.Lock(),
    "warned_cooldown_read_failure": False,
    "warned_cooldown_write_failure": False,
}


# ---------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------


def _utc_now():
    return datetime.now(timezone.utc)


def _format_duration(seconds):
    if seconds is None:
        return "unknown"
    seconds = max(0, int(round(seconds)))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _normalize_timestamp(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _atomic_write_text(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def is_valid_netscape_cookie_file(path):
    if not os.path.exists(path):
        return False
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            first = f.readline().strip()
        return first in ("# Netscape HTTP Cookie File", "# HTTP Cookie File")
    except Exception:
        return False


def resolve_js_runtime(path_hint=""):
    if path_hint:
        expanded = os.path.expanduser(path_hint)
        if os.path.exists(expanded):
            return expanded
    for candidate in ("deno", "node", "bun", "qjs", "quickjs"):
        exe = shutil.which(candidate)
        if exe:
            return exe
    return None


def default_cookie_path(run_dir):
    return os.path.join(run_dir, DEFAULT_COOKIE_RELATIVE_PATH)


def _resolve_profile_path(run_dir):
    return os.path.join(run_dir, PROFILE_FILENAME)


def load_worker_profile(run_dir):
    profile_path = _resolve_profile_path(run_dir)
    if not os.path.exists(profile_path):
        raise RuntimeError(
            f"Missing worker profile at {profile_path}. Run: python setup_worker.py --run-dir \"{run_dir}\""
        )
    try:
        with open(profile_path, "r", encoding="utf-8") as f:
            profile = json.load(f)
    except Exception as exc:
        raise RuntimeError(f"Failed to read worker profile {profile_path}: {exc}") from exc

    if not isinstance(profile, dict):
        raise RuntimeError(f"Invalid worker profile format in {profile_path}.")
    return profile


def save_worker_profile(run_dir, profile):
    profile_path = _resolve_profile_path(run_dir)
    payload = json.dumps(profile, ensure_ascii=True, indent=2)
    _atomic_write_text(profile_path, payload + "\n")
    return profile_path


def build_worker_profile(
    run_dir,
    preset,
    cookie_file,
    js_runtime_path,
    bgutil_server_home,
    max_video_height=360,
):
    if preset not in PRESET_CONFIGS:
        raise ValueError(f"Unknown preset '{preset}'.")

    cfg = PRESET_CONFIGS[preset]
    return {
        "version": 1,
        "created_at": _utc_now().isoformat(),
        "run_dir": os.path.abspath(run_dir),
        "preset": preset,
        "cookie_file": os.path.abspath(cookie_file),
        "js_runtime_path": os.path.abspath(js_runtime_path),
        "bgutil_mode": "script",
        "bgutil_server_home": os.path.abspath(os.path.expanduser(bgutil_server_home)),
        "player_client_primary": ["mweb"],
        "player_client_fallback": ["default"],
        "target_titles_per_hour": int(cfg["target_titles_per_hour"]),
        "jitter_min_seconds": float(cfg["jitter_min_seconds"]),
        "jitter_max_seconds": float(cfg["jitter_max_seconds"]),
        "rate_limit_cooldown_seconds": int(cfg["rate_limit_cooldown_seconds"]),
        "max_rate_limit_retries_per_url": int(cfg["max_rate_limit_retries_per_url"]),
        "token_ttl_hours": int(cfg["token_ttl_hours"]),
        "max_video_height": int(max_video_height),
        "download_retries": 10,
        "fragment_retries": 20,
        "extractor_retries": 5,
        "file_access_retries": 5,
        "concurrent_fragment_downloads": 2,
        "write_info_json": False,
    }


def _package_version(pkg_name):
    try:
        return importlib_metadata.version(pkg_name)
    except Exception:
        return None


def validate_worker_environment(profile):
    missing = []

    js_runtime_path = profile.get("js_runtime_path", "")
    if not js_runtime_path or not os.path.exists(js_runtime_path):
        missing.append("JavaScript runtime path (deno/node/bun/qjs)")

    if importlib.util.find_spec("yt_dlp") is None:
        missing.append("yt_dlp Python package")

    if importlib.util.find_spec("yt_dlp_ejs") is None:
        missing.append("yt_dlp_ejs Python package")

    if _package_version("bgutil-ytdlp-pot-provider") is None:
        missing.append("bgutil-ytdlp-pot-provider Python package")

    bgutil_home = profile.get("bgutil_server_home", "")
    if not bgutil_home or not os.path.isdir(bgutil_home):
        missing.append("bgutil server_home directory")

    cookie_file = profile.get("cookie_file", "")
    if not cookie_file or not is_valid_netscape_cookie_file(cookie_file):
        missing.append("valid Netscape cookie file")

    return {
        "ok": len(missing) == 0,
        "missing": missing,
        "js_runtime_path": js_runtime_path,
        "bgutil_package": _package_version("bgutil-ytdlp-pot-provider"),
        "yt_dlp_ejs": importlib.util.find_spec("yt_dlp_ejs") is not None,
    }


def check_dependencies(allow_continue=True, run_dir=""):
    """
    Validate dependencies for the anti-block pipeline.
    If run_dir is provided, validates the worker profile and cookie file too.
    """
    results = {
        "python_version": sys.version,
        "python_ok": sys.version_info >= (3, 10),
        "ffmpeg_ok": False,
        "ffprobe_ok": False,
        "ytdlp_ok": importlib.util.find_spec("yt_dlp") is not None,
        "js_runtime": None,
        "js_ok": False,
        "yt_dlp_ejs_ok": importlib.util.find_spec("yt_dlp_ejs") is not None,
        "bgutil_ok": _package_version("bgutil-ytdlp-pot-provider") is not None,
        "profile_ok": None,
        "profile_missing": [],
    }

    def exists(cmd):
        try:
            subprocess.run(
                [cmd, "-version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            return True
        except Exception:
            return False

    results["ffmpeg_ok"] = exists("ffmpeg")
    results["ffprobe_ok"] = exists("ffprobe")

    js_runtime = resolve_js_runtime()
    if js_runtime:
        results["js_runtime"] = js_runtime
        results["js_ok"] = True

    if run_dir:
        try:
            profile = load_worker_profile(run_dir)
            profile_check = validate_worker_environment(profile)
            results["profile_ok"] = profile_check["ok"]
            results["profile_missing"] = profile_check["missing"]
        except Exception as exc:
            results["profile_ok"] = False
            results["profile_missing"] = [str(exc)]

    print("\n=== Dependency Check ===")
    print(f"Python version OK:   {results['python_ok']} ({results['python_version']})")
    print(f"yt-dlp installed:     {results['ytdlp_ok']}")
    print(f"yt_dlp_ejs installed: {results['yt_dlp_ejs_ok']}")
    print(f"bgutil provider pkg:  {results['bgutil_ok']}")
    print(f"ffmpeg found:         {results['ffmpeg_ok']}")
    print(f"ffprobe found:        {results['ffprobe_ok']}")
    print(f"JS runtime:           {results['js_runtime']}")
    print(f"JS runtime OK:        {results['js_ok']}")
    if results["profile_ok"] is not None:
        print(f"Worker profile OK:    {results['profile_ok']}")
        if results["profile_missing"]:
            for item in results["profile_missing"]:
                print(f"  - {item}")

    critical_ok = results["python_ok"] and results["ytdlp_ok"]
    anti_block_ok = (
        results["js_ok"]
        and results["yt_dlp_ejs_ok"]
        and results["bgutil_ok"]
        and (results["profile_ok"] in (None, True))
    )

    if not critical_ok:
        print("\nERROR: Critical dependencies missing. Cannot proceed.")
        sys.exit(1)

    if not anti_block_ok:
        if allow_continue:
            print("\nWARNING: Anti-block dependencies are incomplete.")
            print("Run setup_worker.py for full validation and profile setup.")
        else:
            print("\nERROR: Required anti-block dependencies missing.")
            sys.exit(1)

    return results


# ---------------------------------------------------------
# Pipeline initialization
# ---------------------------------------------------------


def initialize_worker_pipeline(
    run_dir,
    shared_cooldown_reader=None,
    shared_cooldown_writer=None,
):
    run_dir = os.path.abspath(run_dir)
    profile = load_worker_profile(run_dir)

    profile_preset = profile.get("preset")
    if profile_preset not in PRESET_CONFIGS:
        raise RuntimeError(
            f"Worker profile has invalid preset '{profile_preset}'. Run setup_worker.py again."
        )

    env_result = validate_worker_environment(profile)
    if not env_result["ok"]:
        missing_text = "\n  - ".join(env_result["missing"])
        raise RuntimeError(
            "Anti-block worker setup is incomplete:\n"
            f"  - {missing_text}\n"
            f"Run: python setup_worker.py --run-dir \"{run_dir}\""
        )

    target_titles_per_hour = max(1, int(profile.get("target_titles_per_hour", 1200)))
    profile["global_min_start_interval_seconds"] = 3600.0 / target_titles_per_hour

    with _pipeline_lock:
        _pipeline_state["configured"] = True
        _pipeline_state["run_dir"] = run_dir
        _pipeline_state["profile"] = profile
        _pipeline_state["cooldown_reader"] = shared_cooldown_reader
        _pipeline_state["cooldown_writer"] = shared_cooldown_writer
        _pipeline_state["next_request_not_before"] = 0.0
        _pipeline_state["local_cooldown_until"] = 0.0
        _pipeline_state["last_cooldown_report_at"] = 0.0
        _pipeline_state["warned_cooldown_read_failure"] = False
        _pipeline_state["warned_cooldown_write_failure"] = False

    return profile


def get_active_pipeline_profile():
    profile = _pipeline_state.get("profile")
    if not _pipeline_state.get("configured") or profile is None:
        return None
    return profile


# ---------------------------------------------------------
# yt-dlp Logging
# ---------------------------------------------------------


class YTDLPLogger:
    def __init__(self):
        self.messages = []

    def debug(self, msg):
        if msg.startswith("[download]"):
            return
        self.messages.append(msg)

    def info(self, msg):
        if msg.startswith("[download]"):
            return
        self.messages.append(msg)

    def warning(self, msg):
        self.messages.append(msg)

    def error(self, msg):
        self.messages.append(msg)

    def get_output(self):
        return "\n".join(self.messages)


# ---------------------------------------------------------
# Pacing and cooldown
# ---------------------------------------------------------


def _read_shared_cooldown_until():
    callback = _pipeline_state.get("cooldown_reader")
    if callback is None:
        return None
    try:
        value = callback()
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value
        else:
            dt = _normalize_timestamp(value)
        if dt is None:
            return None
        return dt.timestamp()
    except Exception as exc:
        if not _pipeline_state["warned_cooldown_read_failure"]:
            print(f"[WARN] Shared cooldown read failed; continuing with local pacing: {exc}")
            _pipeline_state["warned_cooldown_read_failure"] = True
        return None


def _write_shared_cooldown(seconds):
    callback = _pipeline_state.get("cooldown_writer")
    if callback is None:
        return None
    try:
        value = callback(int(seconds))
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value
        else:
            dt = _normalize_timestamp(value)
        if dt is None:
            return None
        return dt.timestamp()
    except Exception as exc:
        if not _pipeline_state["warned_cooldown_write_failure"]:
            print(f"[WARN] Shared cooldown write failed; continuing with local pacing: {exc}")
            _pipeline_state["warned_cooldown_write_failure"] = True
        return None


def _trigger_rate_limit_cooldown(reason, video_id=None):
    profile = _pipeline_state["profile"]
    cooldown_seconds = int(profile.get("rate_limit_cooldown_seconds", 3600))
    now_ts = time.time()
    local_until = now_ts + cooldown_seconds

    with _pipeline_state["cooldown_lock"]:
        _pipeline_state["local_cooldown_until"] = max(
            _pipeline_state["local_cooldown_until"], local_until
        )
        remaining = _pipeline_state["local_cooldown_until"] - now_ts

    shared_until = _write_shared_cooldown(cooldown_seconds)
    if shared_until is not None:
        remaining = max(remaining, shared_until - now_ts)

    target = f" for {video_id}" if video_id else ""
    print(
        f"[WARN] Rate-limit cooldown active for {_format_duration(remaining)}{target}. "
        f"Reason: {reason}"
    )


def _effective_cooldown_until_ts():
    with _pipeline_state["cooldown_lock"]:
        local_until = _pipeline_state["local_cooldown_until"]

    shared_until = _read_shared_cooldown_until()
    if shared_until is None:
        return local_until
    return max(local_until, shared_until)


def _wait_for_cooldown():
    while True:
        now_ts = time.time()
        until_ts = _effective_cooldown_until_ts()
        remaining = max(0.0, until_ts - now_ts)
        if remaining <= 0:
            return

        with _pipeline_state["cooldown_lock"]:
            last_report = _pipeline_state["last_cooldown_report_at"]
            should_report = (
                last_report == 0.0
                or (now_ts - last_report) >= GLOBAL_COOLDOWN_STATUS_INTERVAL_SECONDS
                or remaining <= 1.0
            )
            if should_report:
                _pipeline_state["last_cooldown_report_at"] = now_ts

        if should_report:
            print(
                "[WARN] Global cooldown active | "
                f"remaining={_format_duration(remaining)}"
            )

        sleep_for = min(GLOBAL_COOLDOWN_STATUS_INTERVAL_SECONDS, remaining)
        time.sleep(max(1.0, sleep_for))


def _pace_request_start():
    profile = _pipeline_state["profile"]
    _wait_for_cooldown()

    min_interval = float(profile.get("global_min_start_interval_seconds", 0.0))
    jitter_min = float(profile.get("jitter_min_seconds", 0.0))
    jitter_max = float(profile.get("jitter_max_seconds", 0.0))
    if jitter_max < jitter_min:
        jitter_max = jitter_min

    with _pipeline_state["request_pacing_lock"]:
        now_ts = time.time()
        reserved_start = max(now_ts, _pipeline_state["next_request_not_before"])
        _pipeline_state["next_request_not_before"] = reserved_start + min_interval

    delay = max(0.0, reserved_start - time.time())
    jitter = random.uniform(jitter_min, jitter_max)
    total_sleep = delay + jitter
    if total_sleep > 0:
        time.sleep(total_sleep)

    _wait_for_cooldown()


# ---------------------------------------------------------
# Error classification
# ---------------------------------------------------------


def _is_permanent_error(msg):
    return any(marker in msg for marker in PERMANENT_MARKERS)


def _is_format_error(msg):
    return any(marker in msg for marker in FORMAT_MARKERS)


def _is_rate_limit_error(msg):
    return any(marker in msg for marker in RATE_LIMIT_MARKERS)


# ---------------------------------------------------------
# Download helpers
# ---------------------------------------------------------


def _make_thread_local_cookie_copy(shared_cookie_file):
    tmp_dir = tempfile.mkdtemp(prefix="yt_cookie_")
    tmp_cookie = os.path.join(tmp_dir, "cookies.txt")
    shutil.copy2(shared_cookie_file, tmp_cookie)
    if not is_valid_netscape_cookie_file(tmp_cookie):
        raise RuntimeError(f"Temporary cookie copy is invalid: {tmp_cookie}")
    return tmp_dir, tmp_cookie


def _cleanup_temp_dir(path):
    if path and os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)


def _build_ydl_opts(profile, video_id, download_dir, cookie_file, verbose, player_clients, test):
    os.environ["TOKEN_TTL"] = str(int(profile.get("token_ttl_hours", 6)))

    extractor_args = {
        "youtube": {
            "player_client": player_clients,
        },
        "youtubepot-bgutilscript": {
            "server_home": [profile["bgutil_server_home"]],
        },
    }

    max_h = int(profile.get("max_video_height", 360))
    opts = {
        "format": f"bv*[ext=mp4][height<={max_h}][vcodec!*=av01]+ba[ext=m4a]/"
        f"b[ext=mp4][height<={max_h}][vcodec!*=av01]/"
        f"b[ext=mp4][height<={max_h}]/18",
        "outtmpl": os.path.join(download_dir, f"{video_id}.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "verbose": bool(verbose),
        "writeinfojson": bool(profile.get("write_info_json", False)),
        "skip_download": bool(test),
        "geo_bypass": True,
        "age_limit": 18,
        "retries": int(profile.get("download_retries", 10)),
        "fragment_retries": int(profile.get("fragment_retries", 20)),
        "extractor_retries": int(profile.get("extractor_retries", 5)),
        "file_access_retries": int(profile.get("file_access_retries", 5)),
        "skip_unavailable_fragments": True,
        "concurrent_fragment_downloads": int(profile.get("concurrent_fragment_downloads", 2)),
        "extractor_args": extractor_args,
        "js_runtimes": {
            "deno": {
                "path": profile["js_runtime_path"],
            }
        },
        "cookiefile": cookie_file,
        "http_headers": {
            "User-Agent": "AU_Datalab/5.0",
        },
    }
    return opts


# ---------------------------------------------------------
# Video Download
# ---------------------------------------------------------


def download_video(video_id, download_dir, test=False):
    import yt_dlp

    if not _pipeline_state.get("configured"):
        raise RuntimeError(
            "Downloader pipeline is not configured. "
            "Run start_download.py with a prepared run dir from setup_worker.py."
        )

    profile = _pipeline_state["profile"]
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    os.makedirs(download_dir, exist_ok=True)

    attempts = [
        ("auth-mweb", profile.get("player_client_primary", ["mweb"]), False),
        ("auth-default", profile.get("player_client_fallback", ["default"]), True),
    ]

    rate_limit_retries = 0
    max_rate_limit_retries = int(profile.get("max_rate_limit_retries_per_url", 2))
    final_log = ""

    attempt_index = 0
    while attempt_index < len(attempts):
        label, player_clients, verbose = attempts[attempt_index]
        tmp_dir = None
        tmp_cookie = None
        logger = YTDLPLogger()

        try:
            tmp_dir, tmp_cookie = _make_thread_local_cookie_copy(profile["cookie_file"])

            _pace_request_start()

            ydl_opts = _build_ydl_opts(
                profile=profile,
                video_id=video_id,
                download_dir=download_dir,
                cookie_file=tmp_cookie,
                verbose=verbose,
                player_clients=player_clients,
                test=test,
            )
            ydl_opts["logger"] = logger

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                result_code = ydl.download([video_url])

            final_log = logger.get_output()
            if result_code == 0:
                return "success", None, final_log

            attempt_index += 1

        except yt_dlp.utils.DownloadError as exc:
            final_log = logger.get_output()
            msg = str(exc)
            msg_lower = msg.lower()

            if _is_rate_limit_error(msg_lower):
                rate_limit_retries += 1
                _trigger_rate_limit_cooldown(msg, video_id=video_id)

                if rate_limit_retries <= max_rate_limit_retries:
                    continue

                return "blocked", f"rate_limit: {msg}", final_log

            if _is_permanent_error(msg_lower):
                return "skipped", f"permanent_unavailable: {msg}", final_log

            if _is_format_error(msg_lower):
                attempt_index += 1
                if attempt_index >= len(attempts):
                    return "failure", f"format_error: {msg}", final_log
                continue

            attempt_index += 1
            if attempt_index >= len(attempts):
                return "failure", f"generic_error: {msg}", final_log

        except Exception as exc:
            final_log = logger.get_output()
            attempt_index += 1
            if attempt_index >= len(attempts):
                return "failure", f"generic_error: {exc}", final_log

        finally:
            _cleanup_temp_dir(tmp_dir)

    return "failure", "generic_error: exhausted attempts", final_log
