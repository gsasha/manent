import wx
import wx.wizard

from Config import *


# A wizard for creating new backup rule

class CreateBackupRule(wx.wizard.Wizard):
    def __init__(self, parent, globalConfig):
        wx.wizard.Wizard.__init__(self, parent, -1, "New Backup Rule")
        self.globalConfig = globalConfig
        self.CreateTypeSelectionPage()
        self.CreateBrowsePage()

        self.destination.SetPath("/tmp")
        wx.wizard.WizardPageSimple_Chain(self.typeSelectionPage, self.browsePage)
        
        self.Bind(wx.wizard.EVT_WIZARD_FINISHED, self.OnWizFinished)

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
                    
        self.backupLabel = text1
        return self.typeSelectionPage
    
    def CreateBrowsePage(self):
        self.browsePage = wx.wizard.WizardPageSimple(self)
        win = wx.Panel(self.browsePage)
        
        label2 = wx.StaticText(win, -1, "Select the directory to be backed up")
        self.source = wx.DirPickerCtrl(win, -1)
        self.source.SetPath("/")

        label1 = wx.StaticText(win, -1, "Select the destination directory")
        self.destination = wx.DirPickerCtrl(win, -1)
        self.destination.SetPath("/")

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(label2, 0)
        sizer.Add(self.source, 1, wx.EXPAND)
        sizer.Add((30, 30))
        sizer.Add(label1, 0)
        sizer.Add(self.destination, 1, wx.EXPAND)
        win.SetSizer(sizer)
        
        sizer2 = wx.BoxSizer(wx.HORIZONTAL)
        sizer2.Add(win, 1, wx.ALIGN_CENTER)
        self.browsePage.SetSizer(sizer2)
        
        return self.browsePage
    
    def OnWizFinished(self, e):
		label = self.backupLabel.GetValue()
		source = self.source.GetPath()
		destination = self.destination.GetPath()
		wx.MessageBox("Creating the backup rule %s \n(source:%s, dest:%s)" %(label,source,destination), "Done")

		self.globalConfig.create_backup(label, source, "directory", [destination])
		self.globalConfig.save()

        
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
