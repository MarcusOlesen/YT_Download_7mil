import argparse
import os
import sys

from scraper_utils import (
    DEFAULT_BGUTIL_SERVER_HOME,
    PRESET_CONFIGS,
    build_worker_profile,
    default_cookie_path,
    is_valid_netscape_cookie_file,
    resolve_js_runtime,
    save_worker_profile,
    validate_worker_environment,
)
from worker_auth import capture_youtube_cookies


def parse_args():
    parser = argparse.ArgumentParser(
        description="Set up anti-block worker profile and cookies for this run-dir."
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Worker run directory used by start_download.py.",
    )
    parser.add_argument(
        "--preset",
        default="normal",
        choices=sorted(PRESET_CONFIGS.keys()),
        help="Anti-block preset.",
    )
    parser.add_argument(
        "--cookie-file",
        default="",
        help="Cookie file path. Defaults to <run-dir>\\auth\\youtube_cookies.txt.",
    )
    parser.add_argument(
        "--js-runtime-path",
        default="",
        help="Optional explicit JS runtime path (deno recommended).",
    )
    parser.add_argument(
        "--bgutil-server-home",
        default=DEFAULT_BGUTIL_SERVER_HOME,
        help="Path to bgutil-ytdlp-pot-provider server directory.",
    )
    parser.add_argument(
        "--max-video-height",
        type=int,
        default=360,
        help="Preferred max video height (default 360).",
    )
    parser.add_argument(
        "--reuse-existing-cookies",
        action="store_true",
        help="Skip browser login if cookie file already exists and is valid.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    run_dir = os.path.abspath(args.run_dir)
    os.makedirs(run_dir, exist_ok=True)

    cookie_file = args.cookie_file.strip() or default_cookie_path(run_dir)
    cookie_file = os.path.abspath(os.path.expanduser(cookie_file))

    js_runtime_path = resolve_js_runtime(args.js_runtime_path)
    if not js_runtime_path:
        raise SystemExit(
            "No supported JS runtime found (deno/node/bun/qjs). "
            "Install Deno and retry."
        )

    if args.reuse_existing_cookies and is_valid_netscape_cookie_file(cookie_file):
        print(f"Using existing cookie file: {cookie_file}")
    else:
        print("Starting browser login flow to capture YouTube cookies...")
        capture_youtube_cookies(cookie_file)

    if not is_valid_netscape_cookie_file(cookie_file):
        raise SystemExit(
            f"Cookie file is not valid Netscape format: {cookie_file}"
        )

    profile = build_worker_profile(
        run_dir=run_dir,
        preset=args.preset,
        cookie_file=cookie_file,
        js_runtime_path=js_runtime_path,
        bgutil_server_home=args.bgutil_server_home,
        max_video_height=args.max_video_height,
    )

    env_check = validate_worker_environment(profile)
    if not env_check["ok"]:
        print("Setup failed. Missing requirements:")
        for item in env_check["missing"]:
            print(f"  - {item}")
        print("Install missing dependencies, then run setup_worker.py again.")
        sys.exit(1)

    profile_path = save_worker_profile(run_dir, profile)

    print("\nWorker setup complete.")
    print(f"Run dir:       {run_dir}")
    print(f"Profile file:  {profile_path}")
    print(f"Cookie file:   {cookie_file}")
    print(f"Preset:        {profile['preset']}")
    print(f"JS runtime:    {profile['js_runtime_path']}")
    print(f"bgutil home:   {profile['bgutil_server_home']}")
    print("\nNext step: start your worker via start_supervisor.bat or start_download.py")


if __name__ == "__main__":
    main()
