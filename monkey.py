#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import wraps
import logging
import time

import grpc

from .monitor import Monitor


def add_recovery_interceptor(func, monitor):
    @wraps(func)
    def _(request, context):
        try:
            return func(request, context)
        except Exception as e:
            logging.exception("rpc err")
            try:
                monitor.internal_error(func.__name__, e)
            except:
                logging.exception('report to sentry failed.')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details('An exception with message "%s" was raised!' % e.message)
            #raise grpc.RpcError(e)
            raise e

    return _


def add_monitor_interceptor(func, monitor):
    @wraps(func)
    def _(*args, **kwargs):
        start_time = time.time()
        v = func(*args, **kwargs)
        time_used = int((time.time() - start_time) * 1000)
        monitor.observe(func.__name__, time_used)
        return v

    return _


def patch_server(application_name, port, sentry):
    monitor = Monitor(application_name, port, sentry)

    origin_unary_unary_handler = grpc.unary_unary_rpc_method_handler

    def unary_unary_handler(behavior, request_deserializer=None, response_serializer=None):
        origin_behavior = behavior
        new_behavior = add_recovery_interceptor(
            add_monitor_interceptor(origin_behavior, monitor), monitor)
        return origin_unary_unary_handler(new_behavior, request_deserializer, response_serializer)

    grpc.unary_unary_rpc_method_handler = unary_unary_handler

