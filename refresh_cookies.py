import argparse
import os
import sys

from scraper_utils import (
    is_valid_netscape_cookie_file,
    load_worker_profile,
    save_worker_profile,
    validate_worker_environment,
)
from worker_auth import capture_youtube_cookies


def parse_args():
    parser = argparse.ArgumentParser(
        description="Refresh YouTube cookies for an existing worker profile."
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Worker run directory containing worker_profile.json.",
    )
    parser.add_argument(
        "--cookie-file",
        default="",
        help="Optional cookie file path override.",
    )
    parser.add_argument(
        "--browser-binary",
        default="",
        help=(
            "Optional browser binary for cookie login. If omitted, refresh will "
            "prefer <repo>\\chrome-win\\chrome.exe when available."
        ),
    )
    return parser.parse_args()


def main():
    args = parse_args()
    run_dir = os.path.abspath(args.run_dir)

    profile = load_worker_profile(run_dir)

    cookie_file = args.cookie_file.strip() or profile.get("cookie_file", "")
    if not cookie_file:
        raise SystemExit(
            "No cookie file configured. Run setup_worker.py first."
        )

    cookie_file = os.path.abspath(os.path.expanduser(cookie_file))

    print("Starting browser login flow to refresh cookies...")
    capture_youtube_cookies(cookie_file, browser_binary=args.browser_binary)

    if not is_valid_netscape_cookie_file(cookie_file):
        raise SystemExit(
            f"Refreshed cookie file is invalid Netscape format: {cookie_file}"
        )

    profile["cookie_file"] = cookie_file
    profile_path = save_worker_profile(run_dir, profile)

    env_check = validate_worker_environment(profile)
    if not env_check["ok"]:
        print("Cookie refresh succeeded, but worker environment is still incomplete:")
        for item in env_check["missing"]:
            print(f"  - {item}")
        sys.exit(1)

    print("\nCookie refresh complete.")
    print(f"Profile file: {profile_path}")
    print(f"Cookie file:  {cookie_file}")


if __name__ == "__main__":
    main()
