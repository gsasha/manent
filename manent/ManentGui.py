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
         
         #splitter = wx.SplitterWindow(self, 3)
         
         w1 = wx.Panel(self, style=wx.BORDER_SIMPLE)
         sizer = wx.GridSizer(1)
         dir = wx.GenericDirCtrl(w1, -1)
         sizer.Add(dir, 1, wx.EXPAND )
         w1.SetSizer(sizer)
         w1.SetAutoLayout(1)
         w1.Fit()
         
#         splitter = wx.SplitterWindow(self, 3)
#         mainSizer = wx.BoxSizer(wx.HORIZONTAL)
#         w1 = wx.Window(splitter, style=wx.BORDER_SIMPLE)
#         w2 = wx.Window(splitter, style=wx.BORDER_SIMPLE)
#         w2.SetBackgroundColour('Blue')
#         w2.SetAutoLayout(1)
#         mainSizer.Add(w1)
#         mainSizer.Add(w2)
#         splitter.SetSizer(mainSizer)
#         
#         dir = wx.GenericDirCtrl(w1, -1, size=(200,200))
#         sz = wx.FlexGridSizer(cols=1, hgap=5, vgap=5)
#         
#         sz.Add(dir, 1, wx.EXPAND | wx.ALIGN_RIGHT)
#         w1.SetSizer(sz)
#         w1.SetAutoLayout(True)
#         
#         #text = wx.StaticText(w2, -1, "ADSFADSFDSFDSAFDSA")
#         #text.SetBackgroundColour('Yellow')
#         #sz2 = wx.BoxSizer(wx.HORIZONTAL)
#         #w2.SetSizer(sz2)
#         
#         bdr = wx.BoxSizer(wx.VERTICAL)
#         btn = wx.StaticText(w2, -1, "border")
#         #btn.SetSize((80, 80))
#         bdr.Add(btn, 1, wx.EXPAND|wx.ALL, 150)
#         w2.SetSizer(bdr)
#         bdr.Fit(w2)
#         w2.Fit()
#         btn.SetBackgroundColour('Yellow')
#
#         #sz2.Add(text, 1, wx.EXPAND)
#         w2.SetAutoLayout(True)
#         splitter.SetMinimumPaneSize(20)
#         splitter.SplitVertically(w1, w2, -150)


#         sizer = wx.GridBagSizer(9, 9)
#         sizer.Add(wx.Button(self,-1, "Button"), (0, 0), wx.DefaultSpan,  wx.ALL, 5)
#         sizer.Add(wx.Button(self,-1, "Button"), (1, 1), (1,7), wx.EXPAND)
#         sizer.Add(wx.Button(self,-1, "Button"), (6, 6), (3,3), wx.EXPAND)
#         sizer.Add(wx.Button(self,-1, "Button"), (3, 0), (1,1), wx.ALIGN_CENTER)
#         sizer.Add(wx.Button(self,-1, "Button"), (4, 0), (1,1), wx.ALIGN_LEFT)
#         sizer.Add(wx.Button(self,-1, "Button"), (5, 0), (1,1), wx.ALIGN_RIGHT)
#         sizer.AddGrowableRow(6)
#         sizer.AddGrowableCol(6)
#         self.SetSizerAndFit(sizer)
#         self.Centre()
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
