'''
Created on May 4, 2015

@author: Corentin Berger
'''

from threading          import Lock

from DIRAC.DataManagementSystem.Client.DataLoggingSequence import DataLoggingSequence


class DataLoggingBuffer :
  """ contains all DataLoggingSequence needed by different thread"""

  dict = dict()
  # lock for multi-threading
  lock = Lock()


  def __init__( self ):
    pass


  @classmethod
  def getDataLoggingSequence( cls, threadID ):
    """ return the DataLoggingSequence associated to the threadID
        :param threadID: id of the thread
    """
    cls.lock.acquire()
    if threadID not in cls.dict:
      cls.dict[threadID] = DataLoggingSequence()
    res = cls.dict[threadID]
    cls.lock.release()

    return res


