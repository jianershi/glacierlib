from glaciervault import *

myVault = GlacierVault();
# myVault.addVault("Mavericks-OS-Backup")
# myVault.listVaults();
myVault.getVault("testVault1")
# myVault.resumeUpload("01.mp3")
# myVault.upload("maveric_install_file.tar.gz")
# myVault.upload("maveric_install_file.tar.gz.sha1sum")
myVault.retrieve("01.mp3", wait_mode=False)

# myVault.listJobs();
# myVault.delVault("testVault1")
myVault.drawALine()
print ("localDB:")
myVault.printLocalDB()


