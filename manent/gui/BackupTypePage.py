import wx
import wx.wizard
from WizardUtils import *

class BackupTypePage(wx.wizard.WizardPageSimple):
	def __init__(self, parent):
		wx.wizard.WizardPageSimple.__init__(self, parent)
		titleSizer = CreatePageTitle(self, "Backup type")

		win = wx.Panel(self, -1)
		#win.SetBackgroundColour(wx.Colour(255, 0, 0))

		label1 = wx.StaticText(win, -1, "Name of the backup rule:")
		self.name = wx.TextCtrl(win, -1)
		
		sizer = wx.BoxSizer(wx.VERTICAL)
		sizer.Add(titleSizer, 1, wx.EXPAND|wx.ALL)
		sizer.Add(label1, 0)
		sizer.Add(self.name, 0, wx.EXPAND)
		sizer.Add((30, 30))

		label2 = wx.StaticText(win, -1, "Select the destination type")
		sampleList = ['Local Drive', 'CD/DVD', '(G)Mail', 'RapidShare']
		self.type = wx.ComboBox(win, -1, 
						 sampleList[0], (90, 80), (95, -1), 
						 sampleList, wx.CB_DROPDOWN)
		sizer.Add(label2, 0)
		sizer.Add(self.type, 1, wx.EXPAND)

		win.SetSizer(sizer)
		
		sizer2 = wx.BoxSizer(wx.HORIZONTAL)
		sizer2.Add(win, 1, wx.ALIGN_CENTER)
		self.SetSizer(sizer2)
		
	def GetName(self):
		return self.name
	
	def GetType(self):
		return self.type