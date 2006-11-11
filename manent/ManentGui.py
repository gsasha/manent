import wx
import wx.grid
import wx.wizard

from Backup import Backup
from Config import *
from CreateWizard import *
from wx.lib.wordwrap import wordwrap

class MyFrame(wx.Frame):
    def __init__(self, parent, id, title):
         wx.Frame.__init__(self, parent, id, title, wx.DefaultPosition)

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
         w1.SetAutoLayout(1)
         w1.Fit()
          
         self.CreateDetailGrid(w2)

         sizer2 = wx.BoxSizer(wx.VERTICAL)
         sizer2.Add(self.backupsGrid, 1, wx.EXPAND)         
         button = wx.Button(w2, -1, "Backup Now")
         sizer2.Add(button, 0)
         
         self.Bind(wx.EVT_BUTTON, self.OnDoBackupNowClick, button)
         
         w2.SetSizer(sizer2)
         w2.SetAutoLayout(1)
         w2.Fit()


         splitter.SplitHorizontally(w1, w2, 200)
         
         self.statusBar.SetStatusText("DB: " + os.path.abspath(self.gconfig.homeArea()))
                  
    def InitDb(self):
        self.gconfig = GlobalConfig()
        self.gconfig.load()
         
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
        self.gconfig.close()
        self.Close(1)
         
         
class MyApp(wx.App):
     def OnInit(self):
         frame = MyFrame(None, -1, "Manent")
         frame.SetSize((500, 500))
         frame.Show(True)
         self.SetTopWindow(frame)
         return True

app = MyApp(0)
app.MainLoop()
