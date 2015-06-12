'''
Created on Jun 9, 2015

@author: Corentin Berger
'''

from DIRAC.Core.Base import Script

lfn = None
IDSeq = None
fullFlag = False

Script.registerSwitch( '', 'Full', '   Print full method call' )
Script.registerSwitch( 'f:', 'LFN=', 'Name of LFN [%s]' % lfn )
Script.registerSwitch( 'i:', 'ID=', 'ID of sequence [%s]' % IDSeq )
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'USAGE:',
                                     ' %s [OPTION|CFGFILE] -l LFN -m NAME' % Script.scriptName,
                                     'ARGUMENTS:',
                                     'At least one shall be given\nLFN: AN LFN NAME \ID : A sequence ID' ] ) )

Script.parseCommandLine( ignoreErrors = False )

for switch in Script.getUnprocessedSwitches():
  if switch[0] == "f" or switch[0].lower() == "lfn":
    lfn = switch[1]
  if switch[0] == "i" or switch[0].lower() == "ID":
    IDSeq = switch[1]
  if switch[0].lower() == "full":
    fullFlag = True

from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient

if not lfn and not IDSeq :
  print 'you should give at least one lfn or one sequence ID'
else :
  dlc = DataLoggingClient()
  if lfn :
    res = dlc.getSequenceOnFile( lfn )
    if res['OK']:
      for seq in res['Value'] :
        print seq.printSequenceLFN( lfn, full = fullFlag )
    else :
      print res['Value']
  elif id :
    res = dlc.getSequenceByID( IDSeq )
    if res['OK']:
      for seq in res['Value'] :
        print seq.printSequence( full = fullFlag )
    else :
      print res['Value']
