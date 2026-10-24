from __future__ import annotations

import asyncio
import threading
import time

from communication.lifecycle import AsyncRuntime


def test_first_submission_waits_for_the_event_loop_to_run(monkeypatch):
    original_new_event_loop = asyncio.new_event_loop
    running = threading.Event()

    def delayed_event_loop():
        loop = original_new_event_loop()
        original_run_forever = loop.run_forever
        loop.call_soon(running.set)

        def run_forever():
            # Model the loop thread losing its time slice before run_forever.
            time.sleep(0.05)
            original_run_forever()

        monkeypatch.setattr(loop, "run_forever", run_forever)
        return loop

    monkeypatch.setattr(asyncio, "new_event_loop", delayed_event_loop)
    runtime = AsyncRuntime()
    try:
        assert runtime.run(asyncio.sleep(0, result="published"), timeout=2) == "published"
    finally:
        # Ensure cleanup can stop even the unfixed loop after a failed assertion.
        running.wait(timeout=2)
        runtime.stop(timeout=2)
