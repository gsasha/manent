import os

for file in os.listdir("."):
  try:
    base,index,ext = file.split(".")
    if ext != "mf":
      continue
    new_file = base + "." + index.lower() + "." + ext
    if file != new_file:
      print "Renaming " + file + " to " + new_file
      os.rename(file, new_file)
  except:
    pass
