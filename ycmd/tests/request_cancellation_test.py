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

import threading
from unittest import TestCase

from hamcrest import ( assert_that,
                       calling,
                       contains_exactly,
                       empty,
                       equal_to,
                       raises )

from ycmd.completers.language_server.language_server_protocol import (
  LspRequestId )
from ycmd.request_cancellation import RequestCancellationRegistry


class RecordingConnection:
  def __init__( self ) -> None:
    self.cancelled_request_ids: list[ LspRequestId ] = []


  def CancelRequest( self, request_id: LspRequestId ) -> bool:
    self.cancelled_request_ids.append( request_id )
    return True


class RequestCancellationRegistryTest( TestCase ):
  def setUp( self ) -> None:
    self.registry = RequestCancellationRegistry()


  def test_CancelOperation_CancelsActiveRequestOnce( self ) -> None:
    connection = RecordingConnection()

    with self.registry.CancellableOperation( 1 ) as context:
      with context.TrackRequest( connection, 7 ):
        assert_that(
          self.registry.CancelOperation( 1 ),
          equal_to( True )
        )
        assert_that(
          self.registry.CancelOperation( 1 ),
          equal_to( False )
        )

    assert_that(
      connection.cancelled_request_ids,
      contains_exactly( 7 )
    )


  def test_TrackRequest_CancelsRequestRegisteredAfterCancellation(
      self
  ) -> None:
    connection = RecordingConnection()

    with self.registry.CancellableOperation( 1 ) as context:
      assert_that(
        self.registry.CancelOperation( 1 ),
        equal_to( True )
      )

      with context.TrackRequest( connection, 7 ):
        assert_that(
          context.IsCancellationRequested(),
          equal_to( True )
        )

    assert_that(
      connection.cancelled_request_ids,
      contains_exactly( 7 )
    )


  def test_CancellableOperation_AppliesCancellationReceivedBeforeStart(
      self
  ) -> None:
    connection = RecordingConnection()

    assert_that(
      self.registry.CancelOperation( 1 ),
      equal_to( True )
    )

    with self.registry.CancellableOperation( 1 ) as context:
      assert_that(
        context.IsCancellationRequested(),
        equal_to( True )
      )
      with context.TrackRequest( connection, 7 ):
        pass

    assert_that(
      connection.cancelled_request_ids,
      contains_exactly( 7 )
    )


  def test_CancelOperation_DistinguishesConnectionsWithSameRequestId(
      self
  ) -> None:
    first_connection = RecordingConnection()
    second_connection = RecordingConnection()

    with self.registry.CancellableOperation( 1 ) as context:
      with context.TrackRequest( first_connection, 7 ):
        with context.TrackRequest( second_connection, 7 ):
          self.registry.CancelOperation( 1 )

    assert_that(
      first_connection.cancelled_request_ids,
      contains_exactly( 7 )
    )
    assert_that(
      second_connection.cancelled_request_ids,
      contains_exactly( 7 )
    )


  def test_TrackRequest_RemovesCompletedRequest( self ) -> None:
    connection = RecordingConnection()

    with self.registry.CancellableOperation( 1 ) as context:
      with context.TrackRequest( connection, 7 ):
        pass

      self.registry.CancelOperation( 1 )

    assert_that( connection.cancelled_request_ids, empty() )


  def test_CancellableOperation_RemovesFinishedOperation( self ) -> None:
    with self.registry.CancellableOperation( 1 ):
      pass

    assert_that( self.registry._active_operations, empty() )


  def test_RetireOperations_RemovesObsoletePendingCancellations(
      self
  ) -> None:
    self.registry.CancelOperation( 1 )
    self.registry.CancelOperation( 3 )

    self.registry.RetireOperations( 1 )

    assert_that(
      self.registry._pending_cancellations,
      equal_to( { 3 } )
    )
    assert_that(
      self.registry.CancelOperation( 1 ),
      equal_to( False )
    )


  def test_RetireOperations_DoesNotMoveWatermarkBackward( self ) -> None:
    self.registry.RetireOperations( 5 )
    self.registry.RetireOperations( 3 )

    assert_that(
      self.registry.CancelOperation( 4 ),
      equal_to( False )
    )


  def test_CancellableOperation_RejectsRetiredOperation( self ) -> None:
    self.registry.RetireOperations( 1 )

    assert_that(
      calling( self.registry.CancellableOperation( 1 ).__enter__ ),
      raises( ValueError )
    )


  def test_ConcurrentRegistrationAndCancellation_CancelsExactlyOnce(
      self
  ) -> None:
    connection = RecordingConnection()
    start_race = threading.Barrier( 2 )
    cancellation_finished = threading.Event()
    failures: list[ BaseException ] = []

    with self.registry.CancellableOperation( 1 ) as context:
      def TrackRequest() -> None:
        try:
          start_race.wait()
          with context.TrackRequest( connection, 7 ):
            cancellation_finished.wait()
        except BaseException as exception:
          failures.append( exception )

      thread = threading.Thread( target = TrackRequest )
      thread.start()
      start_race.wait()
      self.registry.CancelOperation( 1 )
      cancellation_finished.set()
      thread.join()

    assert_that( failures, empty() )
    assert_that(
      connection.cancelled_request_ids,
      contains_exactly( 7 )
    )
