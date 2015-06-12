'''
Created on Jun 9, 2015

@author: Corentin Berger
'''
from DIRAC.Core.Base import Script

lfn = None
name = None
fullFlag = False
after = None
before = None

Script.registerSwitch( '', 'Full', '   Print full method call' )
Script.registerSwitch( 'f:', 'LFN=', 'Name of LFN [%s]' % lfn )
Script.registerSwitch( 'm:', 'MethodName=', 'Name of method [%s]' % name )
Script.registerSwitch( 'a:', 'After=', 'date, format be like 1999-12-31 [%s]' % after )
Script.registerSwitch( 'b:', 'Before=', 'date, format be like 1999-12-31 [%s]' % before )
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'USAGE:',
                                     ' %s [OPTION|CFGFILE] -l LFN -m NAME' % Script.scriptName,
                                     'ARGUMENTS:',
                                     'At least one shall be given\nLFN: AN LFN NAME \nNAME : A method name' ] ) )

Script.parseCommandLine( ignoreErrors = False )

for switch in Script.getUnprocessedSwitches():
  if switch[0] == "f" or switch[0].lower() == "lfn":
    lfn = switch[1]
  if switch[0] == "m" or switch[0].lower() == "MethodName":
    name = switch[1]
  if switch[0] == "a" or switch[0].lower() == "After":
    after = switch[1]
  if switch[0] == "b" or switch[0].lower() == "Before":
    before = switch[1]
  if switch[0].lower() == "full":
    fullFlag = True


from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient

args = Script.getPositionalArgs()

dlc = DataLoggingClient()

if not lfn and not name :
  print 'you should give at least one lfn or one method name'
else :
  if lfn :
    res = dlc.getMethodCallOnFile( lfn, before, after )
    if res['OK']:
      for call in res['Value'] :
        print call.printMethodCallLFN( lfn, full = fullFlag )
    else :
      print res['Value']
  elif name :
    res = dlc.getMethodCallByName( name, before, after )
    if res['OK']:
      for call in res['Value'] :
        print call.printMethodCall( full = fullFlag )
    else :
      print res['Value']


