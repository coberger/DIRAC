'''
Created on May 4, 2015

@author: Corentin Berger
'''

from threading          import Lock

from DIRAC.DataManagementSystem.Client.DLSequence import DLSequence


class DLThreadPool :
  """ contains all DLSequence needed by different thread"""

  dict = dict()
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
    if threadID not in cls.dict:
      cls.dict[threadID] = DLSequence()
    res = cls.dict[threadID]
    cls.lock.release()
    return res


