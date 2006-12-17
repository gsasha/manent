import wx
import wx.wizard
from wx import ImageFromStream

from manent.Config import *
from BackupTypePage import *
from BackupFoldersPage import *
from BackupDestinationPage import *

# A wizard for creating new backup rule

class CreateBackupRule(wx.wizard.Wizard):
	def __init__(self, parent, globalConfig):
		wx.wizard.Wizard.__init__(self, parent, -1, "New Backup Rule",
								wx.Bitmap("graphics\WizardLogo.png"))
		self.globalConfig = globalConfig
		
		self.typeSelectionPage = BackupTypePage(self)
		self.destinationPage = BackupDestinationPage(self)
		self.browsePage = BackupFoldersPage(self)
		
		wx.wizard.WizardPageSimple_Chain(self.typeSelectionPage, self.destinationPage)
		wx.wizard.WizardPageSimple_Chain(self.destinationPage, self.browsePage)
		self.Bind(wx.wizard.EVT_WIZARD_FINISHED, self.OnWizFinished)
		   	
	def OnWizFinished(self, e):
		label = self.typeSelectionPage.GetName()
		destination = self.destinationPage.GetDestination()
		
		source = self.browsePage.GetBackupRule()
		
		wx.MessageBox("Creating the backup rule %s \n(source:%s, dest:%s)" %(label,source,destination), "Done")

		self.globalConfig.create_backup(label, 
									source, 
									"directory", [destination])
		self.globalConfig.save()
