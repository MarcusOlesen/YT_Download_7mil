import os
import tempfile
import time


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


def capture_youtube_cookies(cookie_file):
    try:
        import undetected_chromedriver as uc
    except Exception as exc:
        raise RuntimeError(
            "undetected_chromedriver is required for cookie setup. "
            "Install it with: python -m pip install undetected-chromedriver"
        ) from exc

    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = None
    try:
        driver = uc.Chrome(options=options)
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
