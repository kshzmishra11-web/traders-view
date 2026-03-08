import subprocess
import sys
import time
from pathlib import Path

from service_discovery import discover_topic_services


ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable


def main():
    processes = []
    try:
        topic_services = discover_topic_services(ROOT)
        if not topic_services:
            raise RuntimeError("No topic services discovered in topic_services folder")

        for service in topic_services:
            section_name = service["section"]
            script_path = service["script_path"]
            port = service["port"]
            command = [PYTHON, str(script_path)]
            process = subprocess.Popen(command, cwd=ROOT / "topic_services")
            processes.append(process)
            print(f"Started topic-service::{section_name} on port {port}")

        ui_command = [PYTHON, str(ROOT / "traders-view.py")]
        ui_process = subprocess.Popen(ui_command)
        processes.append(ui_process)
        print("Started UI server on http://127.0.0.1:8787")
        print("Press Ctrl+C to stop all services")

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping services...")
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            if process.poll() is None:
                process.wait(timeout=5)


if __name__ == "__main__":
    main()
