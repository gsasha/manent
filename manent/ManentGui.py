import wx
class MyFrame(wx.Frame):
    def __init__(self, parent, id, title):
         wx.Frame.__init__(self, parent, id, title, wx.DefaultPosition)
         
         menuBar = wx.MenuBar()
         fileMenu = wx.Menu();
         menuBar.Append(fileMenu, "File")
         
         fileMenu.Append(1, "Open", "")
         fileMenu.AppendSeparator()
         fileMenu.Append(2, "Exit", "")
         
         wx.EVT_MENU(self, 2, self.OnExit) # attach the menu-event ID_ABOUT to the 
                                   # method self.OnAbout      
         self.SetMenuBar(menuBar)
         self.CreateStatusBar()
         
         splitter = wx.SplitterWindow(self, 3)
         
         w1 = wx.Panel(splitter, style=wx.BORDER_SIMPLE)
         w2 = wx.Panel(splitter, style=wx.BORDER_SIMPLE)
         splitter.Initialize(w1)
         w1.SetBackgroundColour('Yellow')
         sizer = wx.GridSizer(1)
         dir = wx.GenericDirCtrl(w1, -1)
         sizer.Add(dir, 1, wx.EXPAND)
         w1.SetSizer(sizer)
         w1.SetAutoLayout(1)
         w1.Fit()
         wx.StaticText(w2,-1, "Place for notes")
         splitter.SplitVertically(w1, w2, -150)
         
    def OnExit(self, e):
        self.Close(1)
         
         
class MyApp(wx.App):
     def OnInit(self):
         frame = MyFrame(None, -1, "Manent")
         frame.Show(True)
         self.SetTopWindow(frame)
         return True

app = MyApp(0)
app.MainLoop()
