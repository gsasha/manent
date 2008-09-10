from distutils.core import setup

setup(
    name = 'manent',
    description = 'Manent backup software',
    version = '0.10.16',
    author = 'Alex Gontmakher',
    author_email = 'gsasha@gmail.com',
    packages = ['manent', 'manent/utils'],
    py_modules = ['manent'],
    requires = ['paramiko'],
    scripts = ['Manent'],
    )


