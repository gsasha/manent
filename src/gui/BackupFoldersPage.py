import wx
import wx.wizard
from WizardUtils import *
import manent.BackupRule

class BackupFoldersPage(wx.wizard.WizardPageSimple):
	def __init__(self, parent):
		wx.wizard.WizardPageSimple.__init__(self, parent)
		self.backupRule = manent.BackupRule.BackupRule()
		titleSizer = CreatePageTitle(self, "Backup Locations")
		
		pageSizer = wx.BoxSizer(wx.VERTICAL)
		pageSizer.Add(titleSizer, 0, wx.EXPAND|wx.ALL)

		win = wx.Panel(self)
		#win.SetBackgroundColour(wx.Colour(255, 0, 0))
		sizer = wx.BoxSizer(wx.VERTICAL)
		sizer.AddStretchSpacer(1)
		win.SetSizer(sizer)
		pageSizer.Add(win, 12, wx.EXPAND | wx.ALL)

		sizer.Add(wx.StaticText(win, -1, "Right click to select directories for backup"))
		sizer.Add((5, 5))
		self.dirTree = self.CreateTree(win)
		sizer.Add(self.dirTree, 12, wx.EXPAND)

		sizer.AddStretchSpacer(1)
		self.SetSizer(pageSizer)
		
		self.SetupFolderSelection()

	def CreateTree(self, parent):
		tree = wx.GenericDirCtrl(parent, -1, style=wx.DIRCTRL_DIR_ONLY)
		return tree
	
	def SetupFolderSelection(self):
		tree = self.dirTree.GetTreeCtrl()
		tree.Bind(wx.EVT_CONTEXT_MENU, self.OnRightDown)
		
		# register popup
		self.ADD_TO_BACKUP_ID = wx.NewId()
		self.Bind(wx.EVT_MENU, self.OnAddToBackup, id=self.ADD_TO_BACKUP_ID)

	def OnRightDown(self, e):
		tree = self.dirTree.GetTreeCtrl()
		pt = tree.ScreenToClient(e.GetPosition())
		item, flags = tree.HitTest(pt)
		if item:
			tree.SelectItem(item)
			print self.dirTree.GetPath()
			
		menu = wx.Menu()
		item = wx.MenuItem(menu, self.ADD_TO_BACKUP_ID, "Add to backup")
		menu.AppendItem(item)
		self.PopupMenu(menu)
		menu.Destroy()
			
	def OnAddToBackup(self, e):
		self.backupRule.addIncludedTree(self.dirTree.GetPath())
		tree = self.dirTree.GetTreeCtrl();
		tree.SetItemBold(tree.GetSelection(), True)
		print "Added ", self.dirTree.GetPath(), " to backup"
		
	def GetBackupRule(self):
		return self.backupRule
		