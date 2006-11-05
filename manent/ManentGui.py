import wx
import wx.grid
import wx.wizard

from Backup import Backup
from Config import *
from CreateWizard import *

class MyFrame(wx.Frame):
    def __init__(self, parent, id, title):
         wx.Frame.__init__(self, parent, id, title, wx.DefaultPosition)

         self.CreateMenu()
         self.statusBar = self.CreateStatusBar()
         
         splitter = wx.SplitterWindow(self, 3)
         
         w1 = wx.Panel(splitter, style=wx.BORDER_SIMPLE)
         w2 = wx.Panel(splitter, style=wx.BORDER_SIMPLE)
         
         dir = wx.GenericDirCtrl(w1, -1)

         sizer = wx.GridSizer(1)
         sizer.Add(dir, 1, wx.EXPAND)
         w1.SetSizer(sizer)
         w1.SetAutoLayout(1)
         w1.Fit()
          
         self.CreateDetailGrid(w2)

         sizer2 = wx.GridSizer(1)
         sizer2.Add(self.detailGrid, 1, wx.EXPAND)
         w2.SetSizer(sizer2)
         w2.SetAutoLayout(1)
         w2.Fit()

         splitter.SplitVertically(w1, w2, -150)
         
         self.InitDb()
         
    def InitDb(self):
        self.gconfig = GlobalConfig()
        self.gconfig.load()
        self.statusBar.SetStatusText("DB: " + os.path.abspath(self.gconfig.homeArea()))
         
    def CreateMenu(self):
         menuBar = wx.MenuBar()
         fileMenu = wx.Menu();
         menuBar.Append(fileMenu, "File")
         
         fileMenu.Append(1, "Create...", "")
         fileMenu.AppendSeparator()
         fileMenu.Append(2, "Exit", "")
         
         wx.EVT_MENU(self, 2, self.OnExit) # attach the menu-event ID_ABOUT to the 
                                   # method self.OnAbout      
                                   
         wx.EVT_MENU(self, 1, self.OnCreateBackupSet)
         
         self.SetMenuBar(menuBar)
        
    def CreateDetailGrid(self, parent):
        self.detailGrid = wx.grid.Grid(parent, -1)
        self.detailGrid.CreateGrid(20, 5)
        for i in range(20):
            self.detailGrid.SetRowLabelValue(i, "")
        self.detailGrid.SetRowLabelSize(0)
    
    def OnCreateBackupSet(self, parent):        
        wizard = CreateBackupRule(self)
        wizard.RunWizard(wizard.typeSelectionPage)
    
    def OnExit(self, e):
        self.gconfig.close()
        self.Close(1)
         
         
class MyApp(wx.App):
     def OnInit(self):
         frame = MyFrame(None, -1, "Manent")
         frame.Show(True)
         self.SetTopWindow(frame)
         return True

app = MyApp(0)
app.MainLoop()
