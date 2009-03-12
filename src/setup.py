from distutils.core import setup
import glob

extra = {}

# py2exe needs to be installed to work
try:
  import py2exe

  # Help py2exe to find win32com.shell
  try:
    import modulefinder
    #import win32com
    #for p in win32com.__path__[1:]: # Take the path to win32comext
    #    modulefinder.AddPackagePath("win32com", p)
    #pn = "win32com.shell"
    #__import__(pn)
    #m = sys.modules[pn]
    #for p in m.__path__[1:]:
    #    modulefinder.AddPackagePath(pn, p)
  except ImportError:
    traceback.print_exc()
    pass

  extra['console'] = [
      'manent-dispatch.py',
      'manent-integration-test.py',
      'manent-unittest.py']

except ImportError:
    pass

setup(
    name = 'manent',
    description = 'Manent backup software',
    version = 'DEVELOPMENT',
    author = 'Alex Gontmakher',
    data_files = [('testdata', glob.glob('testdata/*.tar'))],
    author_email = 'gsasha@gmail.com',
    packages = ['manent', 'manent/utils'],
    py_modules = ['manent'],
    requires = ['paramiko'],
    **extra
    )


