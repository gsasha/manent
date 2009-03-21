; This file is used to generate installer for manent, by running the following
; commands:
; c:\inst\python26\python setup.py py2exe
; "c:\Program Files\Inno Setup 5\ISCC.exe" manent.iss /O. /Fmanent-setup
[Setup]
AppName=Manent
AppVerName=Manent
DefaultDirName={pf}\Manent
DefaultGroupName=Manent
Compression=lzma
SolidCompression=yes
OutputDir=userdocs:Inno Setup Examples Output

[Files]
Source: "dist\*.*"; DestDir: "{app}"
Source: "scripts\manent.bat"; DestDir: "{win}"
Source: "README-win.txt"; DestDir: "{app}"; Flags: isreadme

[Icons]
Name: "{group}\Manent"; Filename: "{app}\manent-dispatch.exe"
