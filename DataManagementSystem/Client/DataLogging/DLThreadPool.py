'''
Created on May 4, 2015

@author: Corentin Berger
'''

from threading  import Lock

from DIRAC.DataManagementSystem.Client.DataLogging.DLSequence import DLSequence


class DLThreadPool :
  """
    contains all DLSequence needed by different thread
    we need this class because different can be in a sequence, we need to share the sequence
    pool is a dictionary with key thread id and with value a DLSequence
  """
  pool = dict()
  # lock for multi-threading
  lock = Lock()


  def __init__( self ):
    pass


  @classmethod
  def getDataLoggingSequence( cls, threadID ):
    """ return the DLSequence associated to the threadID
        :param threadID: id of the thread
    """
    cls.lock.acquire()
    if threadID not in cls.pool:
      cls.pool[threadID] = DLSequence()
    res = cls.pool[threadID]
    cls.lock.release()
    return res

  @classmethod
  def popDataLoggingSequence( cls, threadID ):
    """ pop an element from the dict and return the value associated to key threadID
        :param threadID: id of the thread
    """
    cls.lock.acquire()
    res = cls.pool.pop( threadID )
    cls.lock.release()
    return res
