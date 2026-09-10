# Copyright (C) 2026 ycmd contributors
#
# This file is part of ycmd.
#
# ycmd is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ycmd is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ycmd.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
import heapq
import threading
from typing import Protocol

from ycmd.completers.language_server.language_server_protocol import (
  LspRequestId )


# These JSON fields connect a YCM operation to the corresponding ycmd handler.
# The retirement watermark means that YCM will never send another operation
# whose ID is less than or equal to that value.
YCM_OPERATION_ID: str = 'operation_id'
YCM_RETIRED_OPERATION_ID: str = 'retired_operation_id'


class CancellableConnection( Protocol ):
  def CancelRequest( self, request_id: LspRequestId ) -> bool:
    ...


@dataclass( frozen = True, eq = False )
class _RegisteredRequest:
  connection: CancellableConnection
  request_id: LspRequestId


  def __hash__( self ) -> int:
    return hash( ( id( self.connection ), self.request_id ) )


  def __eq__( self, other: object ) -> bool:
    if not isinstance( other, _RegisteredRequest ):
      return False

    return ( self.connection is other.connection and
             self.request_id == other.request_id )


@dataclass
class _OperationState:
  context: CancellationContext
  cancellation_requested: bool = False
  requests: set[ _RegisteredRequest ] = field( default_factory = set )


class RequestCancellationRegistry:
  """Coordinates cancellation between YCM operations and downstream requests.

  YCM operation IDs and language-server request IDs belong to different
  namespaces. One YCM operation may register several downstream requests,
  potentially on different language-server connections.
  """

  def __init__( self ) -> None:
    self._lock = threading.Lock()
    self._active_operations: dict[ int, _OperationState ] = {}
    self._pending_cancellations: set[ int ] = set()
    self._pending_cancellation_heap: list[ int ] = []
    self._retired_operation_id: int = -1


  @contextmanager
  def CancellableOperation(
      self,
      operation_id: int
  ) -> Iterator[ CancellationContext ]:
    with self._lock:
      if operation_id <= self._retired_operation_id:
        raise ValueError(
          f'Cannot start retired operation { operation_id }' )

      if operation_id in self._active_operations:
        raise ValueError(
          f'Operation { operation_id } is already active' )

      context = CancellationContext( self, operation_id )
      cancellation_requested = (
        operation_id in self._pending_cancellations
      )
      self._pending_cancellations.discard( operation_id )
      self._active_operations[ operation_id ] = _OperationState(
        context,
        cancellation_requested
      )

    try:
      yield context
    finally:
      self._FinishOperation( context )


  def CancelOperation( self, operation_id: int ) -> bool:
    """Request cancellation of an operation.

    Return True for the first effective cancellation request. Cancellation
    arriving before operation startup is retained until that operation starts.
    """
    requests_to_cancel: tuple[ _RegisteredRequest, ... ] = ()

    with self._lock:
      if operation_id <= self._retired_operation_id:
        return False

      operation = self._active_operations.get( operation_id )
      if operation is None:
        if operation_id in self._pending_cancellations:
          return False

        self._pending_cancellations.add( operation_id )
        heapq.heappush( self._pending_cancellation_heap, operation_id )
      else:
        if operation.cancellation_requested:
          return False

        operation.cancellation_requested = True
        requests_to_cancel = tuple( operation.requests )

    for request in requests_to_cancel:
      request.connection.CancelRequest( request.request_id )

    return True


  def RetireOperations( self, retired_operation_id: int ) -> None:
    """Discard cancellation state for IDs that YCM will never use again."""
    with self._lock:
      if retired_operation_id <= self._retired_operation_id:
        return

      self._retired_operation_id = retired_operation_id

      # Each queued ID is removed at most once. This avoids rescanning all
      # pending cancellations whenever YCM advances the retirement watermark.
      while ( self._pending_cancellation_heap and
              self._pending_cancellation_heap[ 0 ] <= retired_operation_id ):
        operation_id = heapq.heappop( self._pending_cancellation_heap )
        self._pending_cancellations.discard( operation_id )


  def _RegisterRequest(
      self,
      context: CancellationContext,
      connection: CancellableConnection,
      request_id: LspRequestId
  ) -> tuple[ _RegisteredRequest, bool ]:
    request = _RegisteredRequest( connection, request_id )

    with self._lock:
      operation = self._OperationStateForContext( context )
      if request in operation.requests:
        raise ValueError(
          f'Request { request_id } is already registered for operation '
          f'{ context.operation_id }'
        )

      operation.requests.add( request )
      return request, operation.cancellation_requested


  def _UnregisterRequest(
      self,
      context: CancellationContext,
      request: _RegisteredRequest
  ) -> None:
    with self._lock:
      operation = self._OperationStateForContext( context )
      operation.requests.discard( request )


  def _IsCancellationRequested(
      self,
      context: CancellationContext
  ) -> bool:
    with self._lock:
      return self._OperationStateForContext(
        context ).cancellation_requested


  def _FinishOperation( self, context: CancellationContext ) -> None:
    with self._lock:
      operation = self._OperationStateForContext( context )
      if operation.requests:
        raise RuntimeError(
          f'Operation { context.operation_id } finished with active requests'
        )

      del self._active_operations[ context.operation_id ]


  def _OperationStateForContext(
      self,
      context: CancellationContext
  ) -> _OperationState:
    operation = self._active_operations.get( context.operation_id )
    if operation is None or operation.context is not context:
      raise RuntimeError(
        f'Operation { context.operation_id } is no longer active' )

    return operation


class CancellationContext:
  """Operation-scoped interface to the cancellation registry."""

  def __init__(
      self,
      registry: RequestCancellationRegistry,
      operation_id: int
  ) -> None:
    self._registry = registry
    self._operation_id = operation_id


  @property
  def operation_id( self ) -> int:
    return self._operation_id


  def IsCancellationRequested( self ) -> bool:
    return self._registry._IsCancellationRequested( self )


  @contextmanager
  def TrackRequest(
      self,
      connection: CancellableConnection,
      request_id: LspRequestId
  ) -> Iterator[ None ]:
    """Track a downstream request until its terminal response is handled.

    The connection must already know about the request before this method is
    entered. This ensures immediate cancellation can find that request in the
    connection's response table.
    """
    request, cancellation_requested = self._registry._RegisterRequest(
      self,
      connection,
      request_id
    )

    try:
      if cancellation_requested:
        connection.CancelRequest( request_id )
      yield
    finally:
      self._registry._UnregisterRequest( self, request )
