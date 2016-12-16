#!/usr/bin/env python
# -*- coding: utf-8 -*-

import signal
import sys


def register_signal_hanlder(server, max_wait_seconds_before_shutdown=3):

    def graceful_shutdown_handler(signum, frame):
        server.stop(max_wait_seconds_before_shutdown)
        sys.exit(0)

    for sig in (signal.SIGQUIT, signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, graceful_shutdown_handler)