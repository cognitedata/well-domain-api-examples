from datetime import datetime


def log_progress(start_time: datetime, done: int, total: int) -> str:
    done_str = str(done).rjust(len(str(total)))
    ratio = done / total
    if ratio <= 0:
        return f"[{done_str}/{total}]"
    dt = datetime.now() - start_time
    total_time = dt / ratio
    finished = start_time + total_time
    percentage = int(ratio * 100)
    return f"[{done_str}/{total} {percentage:2}% ETA={finished}]"
