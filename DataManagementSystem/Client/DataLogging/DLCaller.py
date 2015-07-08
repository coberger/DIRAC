'''
Created on May 8, 2015

@author: Corentin Berger
'''
from DIRAC.DataManagementSystem.private.DLSerializable import DLSerializable


class DLCaller( DLSerializable ):

  attrNames = ['callerID', 'name']

  def __init__( self, name ):
    super( DLCaller, self ).__init__()
    self.name = name
