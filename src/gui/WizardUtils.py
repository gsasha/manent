import wx
import wx.wizard

def CreatePageTitle(parent, title):
	sizer = wx.BoxSizer(wx.VERTICAL)
	title = wx.StaticText(parent, -1, title)
	title.SetFont(wx.Font(18, wx.SWISS, wx.NORMAL, wx.BOLD))
	sizer.Add(title, 0, wx.ALIGN_CENTRE|wx.ALL, 5)
	sizer.Add(wx.StaticLine(parent, -1), 0, wx.EXPAND|wx.ALL, 5)
		
	return sizer
