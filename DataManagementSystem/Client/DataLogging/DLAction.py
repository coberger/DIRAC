'''
Created on May 4, 2015

@author: Corentin Berger
'''

from DIRAC.DataManagementSystem.private.DLSerializable import DLSerializable

class DLAction( DLSerializable ):
  """
    this is the DLAction class for data logging system
    an action is corresponding to one method call about one lfn
  """

  attrNames = ['fileDL', 'status', 'srcSE', 'targetSE', 'extra', 'errorMessage', 'actionID']

  def __init__( self, fileDL, status, srcSE, targetSE, extra, errorMessage, ID = None ):
    super( DLAction, self ).__init__()
    self.fileDL = fileDL
    self.status = status
    self.srcSE = srcSE
    self.targetSE = targetSE
    self.extra = extra
    self.actionID = ID
    self.methodCallID = None
    self.errorMessage = errorMessage
