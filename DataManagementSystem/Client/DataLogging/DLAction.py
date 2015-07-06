'''
Created on May 4, 2015

@author: Corentin Berger
'''

from DIRAC.DataManagementSystem.private.DLJSON import DLJSON

class DLAction ( DLJSON ):

  attrNames = ['fileDL', 'status', 'srcSE', 'targetSE', 'extra', 'errorMessage', 'actionID']

  def __init__( self, fileDL, status, srcSE, targetSE, extra, errorMessage, ID = None ):
    self.fileDL = fileDL
    self.status = status
    self.srcSE = srcSE
    self.targetSE = targetSE
    self.extra = extra
    self.actionID = ID
    self.methodCallID = None
    self.errorMessage = errorMessage

