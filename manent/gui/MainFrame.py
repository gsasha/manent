import wx
import wx.grid
import wx.wizard
from wx.lib.wordwrap import wordwrap

from manent.Backup import Backup
from manent.Config import *
from gui.CreateWizard import *

class MyFrame(wx.Frame):
	def __init__(self, parent, id, title):
         wx.Frame.__init__(self, parent, id, title, wx.DefaultPosition)
         self.Bind(wx.EVT_CLOSE, self.OnCloseWindow)
         
         self.InitDb()

         self.CreateMenu()
         self.statusBar = self.CreateStatusBar()
         
         splitter = wx.SplitterWindow(self, 3)
         
         w1 = wx.Panel(splitter, style=wx.BORDER_SIMPLE)
         w2 = wx.Panel(splitter, style=wx.BORDER_SIMPLE)
         
         dir = wx.GenericDirCtrl(w1, -1)
         
         sizer = wx.GridSizer(1)
         sizer.Add(dir, 1, wx.EXPAND)
         w1.SetSizer(sizer)
          
         self.CreateDetailGrid(w2)

         sizer2 = wx.BoxSizer(wx.VERTICAL)
         w2.SetSizer(sizer2)
         
         sizer2.Add(self.backupsGrid, 1, wx.EXPAND)
         sizer2.Add(self.CreateButtonsBar(w2), 0)
                  

         splitter.SplitHorizontally(w1, w2, 200)
         
         self.statusBar.SetStatusText("DB: " + os.path.abspath(self.gconfig.home_area()))
                  
	def InitDb(self):
		self.gconfig = GlobalConfig()
		self.gconfig.load()
 
	#
	# Creating the UI
	#
	def CreateMenu(self):
		 menuBar = wx.MenuBar()
		 fileMenu = wx.Menu()
		 menuBar.Append(fileMenu, "File")
		 
		 fileMenu.Append(1, "Create...", "")
		 fileMenu.AppendSeparator()
		 fileMenu.Append(2, "Exit", "")
		 
		 wx.EVT_MENU(self, 2, self.OnExit) # attach the menu-event ID_ABOUT to the 
								   # method self.OnAbout	  
								   
		 wx.EVT_MENU(self, 1, self.OnCreateBackupSet)
		 
		 helpMenu = wx.Menu()
		 menuBar.Append(helpMenu, "Help")
		 helpMenu.Append(3, "About...")
		 wx.EVT_MENU(self, 3, self.OnAboutButton)
		 
		 self.SetMenuBar(menuBar)
        
	def CreateDetailGrid(self, parent):
		self.backupsGrid = wx.grid.Grid(parent, -1)
		self.backupsGrid.CreateGrid(10, 4)
		for i in range(20):
			self.backupsGrid.SetRowLabelValue(i, "")
		self.backupsGrid.SetRowLabelSize(0)
		self.backupsGrid.SetColLabelValue(0, "#")
		self.backupsGrid.SetColLabelValue(1, "Label")
		self.backupsGrid.SetColLabelValue(2, "Source")
		self.backupsGrid.SetColLabelValue(3, "Backup Storage")
		self.FillBackupRulesTable()
    
	def FillBackupRulesTable(self):
		row = 0
		for i in self.gconfig.list_backups():
			self.backupsGrid.SetCellValue(row, 0, str(row + 1))
			self.backupsGrid.SetCellValue(row, 1, i)
			self.backupsGrid.SetCellValue(row, 2, self.gconfig.get_backup(i)[0])
			row = row+1
			
		self.backupsGrid.AutoSizeColumns()
		self.backupsGrid.Fit()
	
	def CreateButtonsBar(self, parent):
		 buttonsPanel = wx.Panel(parent, -1)
		 sizer3 = wx.BoxSizer(wx.HORIZONTAL)
		 buttonsPanel.SetSizer(sizer3)
		 
		 button = wx.Button(buttonsPanel, -1, "Backup Now")
		 sizer3.Add(button, 0)
		 self.Bind(wx.EVT_BUTTON, self.OnDoBackupNowClick, button)

		 button = wx.Button(buttonsPanel, -1, "Restore Now")
		 sizer3.Add(button, 0)
		 self.Bind(wx.EVT_BUTTON, self.OnDoRestoreNowClick, button)

		 button = wx.Button(buttonsPanel, -1, "Info")
		 sizer3.Add(button, 0)
		 self.Bind(wx.EVT_BUTTON, self.OnInfoClick, button)

		 return buttonsPanel
    
    #############
    # Handling the user events
    ############
    
	def OnCreateBackupSet(self, parent):		
		wizard = CreateBackupRule(self, self.gconfig)
		wizard.RunWizard(wizard.typeSelectionPage)
		self.FillBackupRulesTable()
	
	def OnDoBackupNowClick(self, e):
		row = self.backupsGrid.GetGridCursorRow()
		label = self.backupsGrid.GetCellValue(row, 1)
		backup = self.gconfig.load_backup(label)
		backup.scan()
		self.gconfig.save()

	def OnDoRestoreNowClick(self, e):
		row = self.backupsGrid.GetGridCursorRow()
		label = self.backupsGrid.GetCellValue(row, 1)

		dlg = wx.DirDialog(self, "Choose a directory:", 
						style=wx.DD_DEFAULT_STYLE|wx.DD_NEW_DIR_BUTTON)
		
		if dlg.ShowModal() == wx.ID_OK:
			target_path = dlg.GetPath()
			success = 1
		else:
			success = 0
			
		dlg.Destroy()
		if not success:
			return
		
		backup = self.gconfig.load_backup(label)
		backup.restore(target_path)
		self.gconfig.save()
        
	def OnInfoClick(self, e):
		row = self.backupsGrid.GetGridCursorRow()
		label = self.backupsGrid.GetCellValue(row, 1)
		backup = self.gconfig.load_backup(label)
		backup.info()
	
	def OnAboutButton(self, e):
		# First we create and fill the info object
		info = wx.AboutDialogInfo()
		info.Name = "Manent"
		info.Version = "0.0.1-beta"
		info.Copyright = "(C) 2006 Programmers and Coders Everywhere"
		info.Description = wordwrap(
				"This is our description",
			350, wx.ClientDC(self))
		info.WebSite = ("http://en.wikipedia.org/wiki/Hello_world", "Hello World home page")
		info.Developers = [ "Joe Programmer",
							"Jane Coder",
							"Vippy the Mascot" ]

		licenseText = "This is license text"
		info.License = wordwrap(licenseText, 500, wx.ClientDC(self))

		# Then we call wx.AboutBox giving it that info object
		wx.AboutBox(info)
	
	def OnExit(self, e):
		self.Close(1)
		
	def OnCloseWindow(self, e):
		self.gconfig.close()
		self.Destroy()		
