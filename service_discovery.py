from pathlib import Path
import re


SECTION_PATTERN = re.compile(r'^\s*SECTION_NAME\s*=\s*["\'](.+?)["\']\s*$', re.MULTILINE)
PORT_PATTERN = re.compile(r'^\s*PORT\s*=\s*(\d+)\s*$', re.MULTILINE)


def discover_topic_services(root_dir: Path):
    topic_dir = root_dir / "topic_services"
    services = []

    if not topic_dir.exists():
        return services

    for script_path in sorted(topic_dir.glob("*_service.py")):
        try:
            content = script_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue

        section_match = SECTION_PATTERN.search(content)
        port_match = PORT_PATTERN.search(content)
        if not section_match or not port_match:
            continue

        services.append(
            {
                "section": section_match.group(1).strip(),
                "port": int(port_match.group(1)),
                "script_path": script_path,
            }
        )

    services.sort(key=lambda item: item["port"])
    return services


def build_service_urls(root_dir: Path, host: str = "127.0.0.1"):
    return {
        item["section"]: f"http://{host}:{item['port']}"
        for item in discover_topic_services(root_dir)
    }
