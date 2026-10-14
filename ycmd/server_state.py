# Copyright (C) 2013-2020 ycmd contributors
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

from contextlib import AbstractContextManager
import threading
from importlib import import_module
from ycmd.completers.completer import Completer
from ycmd.completers.general.general_completer_store import (
    GeneralCompleterStore )
from ycmd.completers.language_server import (
  generic_lsp_completer,
  language_server_completer,
)
from ycmd.request_cancellation import (
  CancellationContext,
  RequestCancellationRegistry,
)
from ycmd.utils import LOGGER


def _GetGenericLSPCompleter( user_options, filetype ):
  custom_lsp = user_options[ 'language_server' ]
  for server_settings in custom_lsp:
    if filetype in server_settings[ 'filetypes' ]:
      try:
        return generic_lsp_completer.GenericLSPCompleter(
            user_options, server_settings )
      except Exception:
        LOGGER.exception( "Unable to instantiate generic completer for "
                          f"filetype { filetype }" )
        # We might just use a built-in completer
  return None


class ServerState:
  def __init__( self, user_options: dict[ str, object ] ) -> None:
    self._user_options = user_options
    self._filetype_completers = {}
    self._filetype_completers_lock = threading.Lock()
    self._language_server_state_lock = threading.Lock()
    self._language_servers_enabled_by_default: bool = True
    self._gencomp = GeneralCompleterStore( self._user_options )
    self._request_cancellations: RequestCancellationRegistry = (
      RequestCancellationRegistry() )


  @property
  def user_options( self ):
    return self._user_options


  def CancellableOperation(
      self,
      operation_id: int
  ) -> AbstractContextManager[ CancellationContext ]:
    return self._request_cancellations.CancellableOperation( operation_id )


  def CancelOperation( self, operation_id: int ) -> bool:
    return self._request_cancellations.CancelOperation( operation_id )


  def RetireOperations( self, retired_operation_id: int ) -> None:
    self._request_cancellations.RetireOperations( retired_operation_id )


  def Shutdown( self ):
    with self._filetype_completers_lock:
      for completer in self._filetype_completers.values():
        if completer:
          completer.Shutdown()

    self._gencomp.Shutdown()


  def _GetFiletypeCompleterForFiletype(
      self,
      filetype: str
  ) -> Completer | None:
    with self._filetype_completers_lock:
      try:
        return self._filetype_completers[ filetype ]
      except KeyError:
        pass

      completer = _GetGenericLSPCompleter( self._user_options, filetype )

      if completer is None:
        try:
          module = import_module( f'ycmd.completers.{ filetype }.hook' )
          completer = module.GetCompleter( self._user_options )
        except ImportError:
          completer = None

      if (
        isinstance(
          completer,
          language_server_completer.LanguageServerCompleter
        ) and not self._language_servers_enabled_by_default
      ):
        completer.DisableServer()

      supported_filetypes = { filetype }
      if completer:
        supported_filetypes.update( completer.SupportedFiletypes() )

      for supported_filetype in supported_filetypes:
        if supported_filetype not in self._filetype_completers:
          self._filetype_completers[ supported_filetype ] = completer
      return completer


  def GetFiletypeCompleter( self, current_filetypes ):
    completers = [ self._GetFiletypeCompleterForFiletype( filetype )
                   for filetype in current_filetypes ]

    for completer in completers:
      if completer:
        return completer

    raise ValueError(
      f'No semantic completer exists for filetypes: { current_filetypes }' )


  def GetLoadedFiletypeCompleters( self ):
    with self._filetype_completers_lock:
      return { completer for completer in
               self._filetype_completers.values() if completer }


  def SetLanguageServerEnabled(
      self,
      enabled: bool,
      filetype: str | None = None
  ) -> dict[ str, object ]:
    with self._language_server_state_lock:
      if filetype is None:
        with self._filetype_completers_lock:
          self._language_servers_enabled_by_default = enabled
          language_servers = {
            completer
            for completer in self._filetype_completers.values()
            if isinstance(
              completer,
              language_server_completer.LanguageServerCompleter
            )
          }
        affected_filetypes: list[ str ] | None = None
      else:
        completer = self.GetFiletypeCompleter( [ filetype ] )
        if not isinstance(
            completer,
            language_server_completer.LanguageServerCompleter
        ):
          raise ValueError(
            f'The { filetype } semantic completer is not a language server'
          )

        language_servers = { completer }
        affected_filetypes = sorted( {
          filetype,
          *completer.SupportedFiletypes(),
        } )

      progress_cleanups: list[ object ] = []
      for server in language_servers:
        if enabled:
          server.EnableServer()
          continue

        result = server.DisableServer()
        if isinstance( result, dict ):
          cleanup = result.get( 'work_done_progress_cleanup' )
          if cleanup is not None:
            progress_cleanups.append( cleanup )

      LOGGER.info(
        '%s language server%s%s',
        'Enabled' if enabled else 'Disabled',
        's' if filetype is None else '',
        '' if filetype is None else f' for filetype { filetype }'
      )
      return {
        'enabled': enabled,
        'filetypes': affected_filetypes,
        'work_done_progress_cleanups': progress_cleanups,
      }


  def FiletypeCompletionAvailable( self, filetypes, silent = False ):
    """Returns True if there is a ycmd semantic completer defined for any
    filetype in the list |filetypes|. Otherwise, returns False and prints an
    error to the log file, unless silent = True."""
    try:
      self.GetFiletypeCompleter( filetypes )
      return True
    except Exception:
      if not silent:
        LOGGER.exception( 'Semantic completion not available for %s',
                          filetypes )
      return False


  def FiletypeCompletionUsable(
      self,
      filetypes: list[ str ],
      silent: bool = False
  ) -> bool:
    """Return True if ycmd supports semantic compltion for any filetype in the
    list |filetypes| and those filetypes are not disabled by user options."""
    if not self.CurrentFiletypeCompletionEnabled( filetypes ):
      return False

    try:
      completer = self.GetFiletypeCompleter( filetypes )
    except Exception:
      if not silent:
        LOGGER.exception( 'Semantic completion not available for %s',
                          filetypes )
      return False

    return not (
      isinstance(
        completer,
        language_server_completer.LanguageServerCompleter
      ) and completer.ServerIsDisabled()
    )


  def ShouldUseFiletypeCompleter( self, request_data ):
    """Determines whether or not the semantic completion should be called for
    completion request."""
    filetypes = request_data[ 'filetypes' ]
    if not self.FiletypeCompletionUsable( filetypes ):
      # don't use semantic, ignore whether or not the user requested forced
      # completion as that's not relevant to signatures.
      return False

    if request_data[ 'force_semantic' ]:
      # use semantic, and it was forced
      return True

    filetype_completer = self.GetFiletypeCompleter( filetypes )
    # was not forced. check the conditions for triggering
    return filetype_completer.ShouldUseNow( request_data )


  def GetGeneralCompleter( self ):
    return self._gencomp


  def CurrentFiletypeCompletionEnabled( self, current_filetypes ):
    """Return False if all filetypes in the list |current_filetypes| are
    disabled by the user option 'filetype_specific_completion_to_disable'."""
    filetype_to_disable = self._user_options[
        'filetype_specific_completion_to_disable' ]
    if '*' in filetype_to_disable:
      return False
    else:
      return not all( x in filetype_to_disable for x in current_filetypes )
