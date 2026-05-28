import zipfile
src='job_7faa.jar'
dst='job_7faa_nofat.jar'
with zipfile.ZipFile(src,'r') as zin:
    names=zin.namelist()
    with zipfile.ZipFile(dst,'w') as zout:
        for n in names:
            if n.startswith('com/fasterxml'):
                continue
            zout.writestr(n, zin.read(n))
print('wrote',dst)
