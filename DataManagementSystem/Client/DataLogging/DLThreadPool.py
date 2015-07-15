'''
Created on May 4, 2015

@author: Corentin Berger
'''
import socket
from threading  import Lock

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.DataManagementSystem.Client.DataLogging.DLSequence import DLSequence
from DIRAC.DataManagementSystem.Client.DataLogging.DLUserName import DLUserName
from DIRAC.DataManagementSystem.Client.DataLogging.DLGroup import DLGroup
from DIRAC.DataManagementSystem.Client.DataLogging.DLHostName import DLHostName

class DLThreadPool :
  """
    contains all DLSequence needed by different thread
    this class serve to have one sequence by thread
    pool is a dictionary with key thread id and with value a DLSequence
  """
  pool = dict()
  # lock for multi-threading
  lock = Lock()


  def __init__( self ):
    pass


  @classmethod
  def getDataLoggingSequence( cls, threadID ):
    """
      return the DLSequence associated to the threadID

      :param threadID: id of the thread

      :return res, S_OK( sequence ) or S_ERROR('Error message')
    """
    cls.lock.acquire()
    if threadID not in cls.pool:
      seq = DLSequence()
      res = getProxyInfo()
      if res['OK']:
        proxyInfo = res['Value']
        seq.userName = DLUserName( proxyInfo.get( 'username' ) )
        seq.group = DLGroup( proxyInfo.get( 'group' ) )
      seq.hostName = DLHostName( socket.gethostname() )
      cls.pool[threadID] = seq
    res = cls.pool[threadID]
    cls.lock.release()
    return res

  @classmethod
  def popDataLoggingSequence( cls, threadID ):
    """
      pop an element from the dict and return the value associated to key threadID

      :param threadID: id of the thread
    """
    cls.lock.acquire()
    res = cls.pool.pop( threadID )
    cls.lock.release()
    return res
