#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.version import StrictVersion
from prometheus_client import Counter, Histogram, start_http_server


class Monitor(object):

    def __init__(self, application_name, port, sentry):
        self.application_name = application_name
        self.port = port
        self.sentry = sentry

        self.req_counter = Counter('%s_requests_total' % application_name,
                                   'Total request counts',
                                   ['method', 'endpoint', 'process'])
        self.err_counter = Counter('%s_error_total' % application_name,
                                   'Total error counts',
                                   ['method', 'endpoint', 'process'])
        self.resp_latency = Histogram('%s_response_latency_millisecond' % application_name,
                                    'Response latency (millisecond)',
                                    ['method', 'endpoint', 'process'],
                                    buckets=(10, 20, 30, 50, 80, 100, 200, 300, 500, 1000, 2000, 3000))

    def internal_error(self, method_name, e):
        self.err_counter.labels(**self.labels(method_name)).inc()
        self.sentry.captureException()

    def labels(self, method_name):
        return {
                'method': 'rpc',
                'endpoint': method_name,
                'process': self.port
            }

    def observe(self, method_name, time_used):
        labels = self.labels(method_name)
        self.req_counter.labels(**labels).inc()
        self.resp_latency.labels(**labels).observe(time_used)


def start_metrics_server(metrics_server_port):
    start_http_server(metrics_server_port)