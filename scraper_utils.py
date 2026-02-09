# ---------------------------------------------------------
# Dependency Validation
# ---------------------------------------------------------

def check_dependencies(allow_continue=True):
    """
    Validate presence of dependencies:
    - Python 3.10+
    - ffmpeg, ffprobe
    - yt-dlp
    - JavaScript runtime (deno or node)

    Returns: dict describing detected capabilities.
    """
    import sys, subprocess    
    
    results = {
        "python_version": sys.version,
        "python_ok": sys.version_info >= (3, 10),
        "ffmpeg_ok": False,
        "ffprobe_ok": False,
        "ytdlp_ok": False,
        "js_runtime": None,
        "js_ok": False,
    }

    # Check yt-dlp
    try:
        import yt_dlp  # noqa
        results["ytdlp_ok"] = True
    except Exception:
        results["ytdlp_ok"] = False

    # ffmpeg / ffprobe
    def exists(cmd):
        try:
            subprocess.run([cmd, "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return True
        except Exception:
            return False

    results["ffmpeg_ok"] = exists("ffmpeg")
    results["ffprobe_ok"] = exists("ffprobe")

    # JS runtime
    if exists("deno"):
        results["js_runtime"] = "deno"
        results["js_ok"] = True
    elif exists("node"):
        results["js_runtime"] = "node"
        results["js_ok"] = True

    # Print diagnostic
    print("\n=== Dependency Check ===")
    print(f"Python version OK:   {results['python_ok']} ({results['python_version']})")
    print(f"yt-dlp installed:     {results['ytdlp_ok']}")
    print(f"ffmpeg found:         {results['ffmpeg_ok']}")
    print(f"ffprobe found:        {results['ffprobe_ok']}")
    print(f"JS runtime:           {results['js_runtime']}")
    print(f"JS runtime OK:        {results['js_ok']}")

    if not all([results["python_ok"], results["ytdlp_ok"]]):
        print("\nERROR: Critical dependencies missing. Cannot proceed.")
        sys.exit(1)

    # Non-critical warnings
    if not all([results["ffmpeg_ok"], results["js_ok"]]):
        if allow_continue:
            print("\nSome optional dependencies missing. You can continue, but downloading may be affected.")
            print("Consider installing missing components for best performance.")
            print("See https://github.com/yt-dlp/yt-dlp/tree/master?tab=readme-ov-file#dependencies")
        else:
            print("\nERROR: Required optional components missing.")
            sys.exit(1)

    return results


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
# Video Download
# ---------------------------------------------------------

def download_video(video_id, download_dir, test=False):
    import yt_dlp as yt, os
    
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    os.makedirs(download_dir, exist_ok=True)

    logger = YTDLPLogger()
    ydl_opts = {
        'format': 'bv*[ext=mp4][height<=360][vcodec!*=av01]+ba[ext=m4a]/b[ext=mp4][height<=360][vcodec!*=av01]/b[ext=mp4][height<=360]/18',
        'outtmpl': os.path.join(download_dir, f'{video_id}.%(ext)s'),
        'noplaylist': True,
        'quiet': True,
        'verbose': False,
        'writeinfojson': False,
        'skip_download': test,
        'geo_bypass': True,
        'age_limit': 18,
        'retries': 3,
        'logger': logger,
        'http_headers': {'User-Agent': 'AU_Datalab/3.0'},
    }

    try:
        with yt.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        return "success", None, logger.get_output()
    except Exception as e:
        return "failure", str(e), logger.get_output()

