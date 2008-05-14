import wx
import wx.wizard
from WizardUtils import *

class BackupDestinationPage(wx.wizard.WizardPageSimple):
	def __init__(self, parent, needCredentials=True):
		wx.wizard.WizardPageSimple.__init__(self, parent)

		titleSizer = CreatePageTitle(self, "Backup Destination")

		pageSizer = wx.BoxSizer(wx.VERTICAL)
		pageSizer.Add(titleSizer, 0, wx.EXPAND|wx.ALL)

		win = wx.Panel(self)
		#win.SetBackgroundColour(wx.Colour(255, 0, 0))
		sizer = wx.BoxSizer(wx.VERTICAL)
		sizer.AddStretchSpacer(1)
		win.SetSizer(sizer)
		pageSizer.Add(win, 1, wx.EXPAND | wx.ALL)

		destinationLabel = wx.StaticText(win, -1, "Destination:");
		self.destination = wx.DirPickerCtrl(win, -1)
		#self.destination.SetPath("")
		sizer.Add(destinationLabel, 0, wx.ALIGN_LEFT)
		sizer.Add(self.destination, 0, wx.EXPAND)
		sizer.Add((50, 50))

		self.needCredentials = needCredentials
		if (needCredentials):
			loginLabel = wx.StaticText(win, -1, "Login ID:");
			self.loginText = wx.TextCtrl(win, -1, "")
			passwordLabel = wx.StaticText(win, -1, "Password:");
			self.passwordText = wx.TextCtrl(win, -1, "", style=wx.TE_PASSWORD)
			
			sizer.Add(loginLabel, 0, wx.ALIGN_LEFT)
			sizer.Add(self.loginText, 0, wx.EXPAND)
			sizer.Add((20, 20))
			sizer.Add(passwordLabel, 0, wx.ALIGN_LEFT)
			sizer.Add(self.passwordText, 0, wx.EXPAND)
			sizer.Add((50, 50))

		sizer.AddStretchSpacer(1)
		self.SetSizer(pageSizer)

	def GetDestination(self):
		return self.destination.GetPath()
	
	def GetUserName(self):
		return self.loginText.GetText()
	
	def GetPassword(self):
		return self.passwordText.GetText()