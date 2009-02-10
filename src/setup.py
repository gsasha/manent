from distutils.core import setup

setup(
    name = 'manent',
    description = 'Manent backup software',
    version = 'DEVELOPMENT',
    author = 'Alex Gontmakher',
    author_email = 'gsasha@gmail.com',
    packages = ['manent', 'manent/utils'],
    py_modules = ['manent'],
    requires = ['paramiko'],
    scripts = ['scripts/manent'],
    )


