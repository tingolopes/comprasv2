from datetime import datetime


def _ts() -> str:
    return datetime.now().strftime('%H:%M:%S')


def log_event(level: str, context: str, message: str, **fields):
    """Linha única e padronizada para logs de execução."""
    extras = " | ".join(f"{k}={v}" for k, v in fields.items())
    base = f"[{_ts()}] {level:<5} | {context:<14} | {message}"
    print(f"{base} | {extras}" if extras else base)


def log_section(title: str):
    print(f"\n[{_ts()}] INFO  | SECTION        | {title}")


def log_banner(title: str):
    print(f"\n[{_ts()}] INFO  | BANNER         | {title}")
