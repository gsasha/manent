import wx
import wx.wizard

# A wizard for creating new backup rule

class CreateBackupRule(wx.wizard.Wizard):
    def __init__(self, parent):
        wx.wizard.Wizard.__init__(self, parent, -1, "New Backup Rule")
        self.CreateTypeSelectionPage()
        self.CreateBrowsePage()
        
        wx.wizard.WizardPageSimple_Chain(self.typeSelectionPage, self.browsePage)

    def CreateTypeSelectionPage(self):
        self.typeSelectionPage = wx.wizard.WizardPageSimple(self)
        win = wx.Panel(self.typeSelectionPage, -1)

        label1 = wx.StaticText(win, -1, "Name of the backup rule:")
        text1 = wx.TextCtrl(win, -1)
        
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(label1, 0)
        sizer.Add(text1, 1, wx.EXPAND)
        sizer.Add((30, 30))

        
        label2 = wx.StaticText(win, -1, "Select the destination type")
        sampleList = ['Local Drive', 'CD/DVD', '(G)Mail', 'RapidShare']
        cb = wx.ComboBox(win, -1, 
                         sampleList[0], (90, 80), (95, -1), 
                         sampleList, wx.CB_DROPDOWN)
        sizer.Add(label2, 0)
        sizer.Add(cb, 1, wx.EXPAND)

        win.SetSizer(sizer)
        
        sizer2 = wx.BoxSizer(wx.HORIZONTAL)
        sizer2.Add(win, 1, wx.ALIGN_CENTER)
        self.typeSelectionPage.SetSizer(sizer2)
                    
        return self.typeSelectionPage
    
    def CreateBrowsePage(self):
        self.browsePage = wx.wizard.WizardPageSimple(self)
        win = wx.Panel(self.browsePage)
        
        label1 = wx.StaticText(win, -1, "Select the destination folder")
        picker = wx.DirPickerCtrl(win, -1)

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(label1, 0)
        sizer.Add(picker, 1, wx.EXPAND)
        win.SetSizer(sizer)
        
        sizer2 = wx.BoxSizer(wx.HORIZONTAL)
        sizer2.Add(win, 1, wx.ALIGN_CENTER)
        self.browsePage.SetSizer(sizer2)
        
        return self.browsePage
        
        
#    def CreateFileSelectionPage(self):
#        
#        win = wx.Panel(page, -1)
#        
#        label1 = wx.StaticText(win, -1, "Name of the backup rule:")
#        text1 = wx.TextCtrl(win, -1)
#        
#        sizer = wx.BoxSizer(wx.VERTICAL)
#        sizer.Add(label1, 0)
#        sizer.Add(text1, 1, wx.EXPAND)
#        win.SetSizer(sizer)
#        
#        sizer2 = wx.BoxSizer(wx.HORIZONTAL)
#        sizer2.Add(win, 1, wx.ALIGN_CENTER)
#        page1.SetSizer(sizer2)
#        
#        sizer.Add()
