import os
import shutil
import tempfile
import time
from pathlib import Path


COOKIE_HEADER = "# Netscape HTTP Cookie File\n"


def _atomic_write(path, text):
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


def serialize_cookies_to_netscape(cookies):
    lines = [COOKIE_HEADER.rstrip("\n")]
    for cookie in cookies:
        domain = cookie.get("domain", "")
        if not domain:
            continue
        flag = "TRUE" if domain.startswith(".") else "FALSE"
        path = cookie.get("path", "/")
        secure = "TRUE" if cookie.get("secure", False) else "FALSE"
        expiry = str(int(cookie.get("expiry", time.time() + 365 * 24 * 3600)))
        name = cookie.get("name", "")
        value = cookie.get("value", "")
        lines.append(
            f"{domain}\t{flag}\t{path}\t{secure}\t{expiry}\t{name}\t{value}"
        )
    return "\n".join(lines) + "\n"


def _find_chrome_binary(browser_binary=""):
    if browser_binary:
        explicit = os.path.abspath(os.path.expanduser(browser_binary))
        if os.path.exists(explicit):
            return explicit
        raise RuntimeError(
            f"Configured browser binary does not exist: {explicit}"
        )

    repo_root = Path(__file__).resolve().parent
    repo_local_candidates = [
        repo_root / "chrome-win" / "chrome.exe",
        repo_root / "browser" / "chrome.exe",
        repo_root / "browser" / "msedge.exe",
    ]
    for candidate in repo_local_candidates:
        if candidate.exists():
            return str(candidate.resolve())

    env_candidates = [
        os.getenv("CHROME_BINARY"),
        os.getenv("GOOGLE_CHROME_BIN"),
        os.getenv("CHROME_BIN"),
    ]
    for candidate in env_candidates:
        if candidate and os.path.exists(candidate):
            return os.path.abspath(candidate)

    for candidate in ("chrome", "chrome.exe", "msedge", "msedge.exe"):
        resolved = shutil.which(candidate)
        if resolved:
            return os.path.abspath(resolved)

    local_app_data = os.getenv("LOCALAPPDATA", "")
    program_files = os.getenv("PROGRAMFILES", r"C:\Program Files")
    program_files_x86 = os.getenv("PROGRAMFILES(X86)", r"C:\Program Files (x86)")

    path_candidates = [
        Path(local_app_data) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(program_files) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(program_files_x86) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(program_files) / "Chromium" / "Application" / "chrome.exe",
        Path(program_files_x86) / "Chromium" / "Application" / "chrome.exe",
        Path(local_app_data) / "Chromium" / "Application" / "chrome.exe",
        Path(local_app_data) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        Path(program_files) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        Path(program_files_x86) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
    ]
    for candidate in path_candidates:
        if candidate.exists():
            return str(candidate.resolve())

    return None


def capture_youtube_cookies(cookie_file, browser_binary=""):
    try:
        import undetected_chromedriver as uc
    except Exception as exc:
        raise RuntimeError(
            "undetected_chromedriver is required for cookie setup. "
            "Install it with: python -m pip install undetected-chromedriver"
        ) from exc

    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")

    chrome_binary = _find_chrome_binary(browser_binary)
    if not chrome_binary:
        raise RuntimeError(
            "Could not locate a Chrome/Chromium/Edge executable. "
            "You can bundle one at <repo>\\chrome-win\\chrome.exe or set "
            "CHROME_BINARY / --browser-binary to a full browser path."
        )
    options.binary_location = chrome_binary

    driver = None
    try:
        driver = uc.Chrome(options=options, browser_executable_path=chrome_binary)
        driver.get("https://www.youtube.com")

        print("\n" + "=" * 72)
        print("ACTION REQUIRED: YouTube login")
        print("1. A Chrome window has opened.")
        print("2. Log in to the machine-specific Google account.")
        print("3. Complete 2FA/CAPTCHA if prompted.")
        print("4. Wait until YouTube homepage is fully loaded.")
        print("=" * 72 + "\n")

        input("Press ENTER here after login is complete... ")

        cookies = driver.get_cookies()
        payload = serialize_cookies_to_netscape(cookies)
        _atomic_write(cookie_file, payload)
        return cookie_file
    finally:
        if driver is not None:
            driver.quit()
