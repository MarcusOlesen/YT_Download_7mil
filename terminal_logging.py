import os
import re
import sys
import threading
from datetime import datetime, timezone


def _sanitize_filename_part(value):
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    text = text.strip("._")
    return text or "session"


class _TeeStream:
    def __init__(self, stream, log_file, lock):
        self._stream = stream
        self._log_file = log_file
        self._lock = lock
        self.encoding = getattr(stream, "encoding", "utf-8")
        self.errors = getattr(stream, "errors", "strict")

    def write(self, data):
        if not data:
            return 0
        text = str(data)
        with self._lock:
            written = self._stream.write(text)
            self._log_file.write(text)
            self._stream.flush()
            self._log_file.flush()
        if written is None:
            return len(text)
        return written

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def flush(self):
        with self._lock:
            self._stream.flush()
            self._log_file.flush()

    def isatty(self):
        return self._stream.isatty()

    def fileno(self):
        return self._stream.fileno()

    def writable(self):
        return True

    def __getattr__(self, name):
        return getattr(self._stream, name)


class TerminalLogSession:
    def __init__(self, log_path):
        self.log_path = os.path.abspath(log_path)
        self._log_file = None
        self._orig_stdout = None
        self._orig_stderr = None
        self._stdout_tee = None
        self._stderr_tee = None

    def start(self):
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        self._log_file = open(self.log_path, "a", encoding="utf-8", buffering=1)
        self._orig_stdout = sys.stdout
        self._orig_stderr = sys.stderr
        lock = threading.RLock()
        self._stdout_tee = _TeeStream(self._orig_stdout, self._log_file, lock)
        self._stderr_tee = _TeeStream(self._orig_stderr, self._log_file, lock)
        sys.stdout = self._stdout_tee
        sys.stderr = self._stderr_tee
        print(f"Logging terminal output to {self.log_path}")
        return self

    def close(self):
        if self._orig_stdout is None:
            return
        if sys.stdout is self._stdout_tee:
            sys.stdout = self._orig_stdout
        if sys.stderr is self._stderr_tee:
            sys.stderr = self._orig_stderr
        self._log_file.flush()
        self._log_file.close()
        self._log_file = None
        self._orig_stdout = None
        self._orig_stderr = None
        self._stdout_tee = None
        self._stderr_tee = None


def start_terminal_logging(script_name, log_dir, *name_parts):
    safe_script_name = _sanitize_filename_part(script_name)
    safe_parts = [_sanitize_filename_part(part) for part in name_parts if part]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename_parts = [safe_script_name] + safe_parts + [timestamp]
    log_path = os.path.join(log_dir, "_".join(filename_parts) + ".log")
    return TerminalLogSession(log_path).start()
