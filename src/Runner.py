# This is the main runner of the GUI.
from gui.MainFrame import *

class ApplicationUI(wx.App):
	 def OnInit(self):
		 frame = MainFrame(None, -1, "Manent")
		 frame.SetSize((500, 500))
		 frame.Show(True)
		 self.SetTopWindow(frame)
		 return True

app = ApplicationUI(None)
app.MainLoop()
