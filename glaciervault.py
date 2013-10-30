import boto
import boto.glacier.layer2
import shelve
import sys
import json
import datetime
import os.path
from boto.glacier.exceptions import UnexpectedHTTPResponseError, ArchiveError, DownloadArchiveError, TreeHashDoesNotMatchError, UploadArchiveError
from credentials import aws_access_key_id, aws_secret_access_key

SHELVE_FILE = ".glaciervault.db"

#global functions
def json_pretty_print(jsonstring):
	print json.dumps(jsonstring, sort_keys=True, indent=4, separators=(',', ': '))
def 	ALine(self):
	print "-------------------------------------------------------------------------------------------------------------------------------------------------------------------"

class glacier_shelve(object):
	"""
	Context manager for shelve
	"""
 
	def __enter__(self):
		self.shelve = shelve.open(SHELVE_FILE)
 
		return self.shelve
 
	def __exit__(self, exc_type, exc_value, traceback):
		self.shelve.close()


class GlacierVault:
	"""
	Wrapper for uploading/download archive to/from Amazon Glacier Vault
	Makes use of shelve to store archive id corresponding to filename and waiting jobs.
 
	Backup:
	>>> GlacierVault("myvault")upload("myfile")
	
	Restore:
	>>> GlacierVault("myvault")retrieve("myfile")
 
	or to wait until the job is ready:
	>>> GlacierVault("myvault")retrieve("serverhealth2.py", True)
	"""

	def __init__(self):
		"""
		Initialize the vault
		"""
		self.layer2 = boto.glacier.layer2.Layer2(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, account_id='-', is_secure=True, port=None, proxy=None, proxy_port=None, proxy_user=None, proxy_pass=None, debug=0, https_connection_factory=None, path='/', provider='aws', security_token=None, suppress_consec_slashes=True, region=None, region_name='us-west-1')
		self.vault = None


	def isVaultExistInLocalDB(self,vault_name):
		with glacier_shelve() as d:
			if not d.has_key("vaults"):
				d["vaults"]=dict()
				return False
			else:
				if vault_name in d["vaults"]:
					return True
				else:
					return False

	def getVault(self,vault_name):
		try:
			self.vault = self.layer2.get_vault(vault_name)
			#this makes sure any program proceed will assume it has a copy in the local database
			if not self.isVaultExistInLocalDB:
				raise Exception("Vault is not present in local database, file structure is not available")

			print ("valt {} selected").format(self.vault.name)

		except UnexpectedHTTPResponseError:
			print ("vault doesnt exist in glacier. the current list of vault:")
			self.listVaults();
			print ("vault doesn't exist in glacier")


	def toDateReadable(self,aUnixTimeStamp):
		aDate=datetime.datetime.strptime(aUnixTimeStamp,"%Y-%m-%dT%H:%M:%S.%fZ")	
		return aDate.strftime("%m/%d/%y %I:%M:%S %p %Z")

	def listVaults(self):
		a_list_of_vaults = self.layer2.list_vaults();

		drawALine()
		for vault in a_list_of_vaults:
			aCreationDate = None
			aLastInventoryDate = None
			aName = None
			aSize = None
			aCount = None
			if (vault.creation_date):
				aCreationDate = self.toDateReadable(vault.creation_date)
			if (vault.last_inventory_date):
				aLastInventoryDate = self.toDateReadable(vault.last_inventory_date)
			if (vault.name):
				aName = vault.name
			if (vault.size):
				aSize = str(int(vault.size)/1024/1024)
			if (vault.number_of_archives):
				aCount = vault.number_of_archives

			print "name: {} | created: {} | last inventory: {} | size:{} MB | number of archieves:{} ".format(aName, aCreationDate, aLastInventoryDate, aSize, aCount)
			drawALine()


	"""
	add a new vault with name as vault_name
	"""
	def addVault(self,vault_name):
		a_list_of_vaults = self.layer2.list_vaults();

		#check if the same name exist
		for vault in a_list_of_vaults:
			if vault.name == vault_name:
				raise Exception("vault with this name already exist, please rename it")

		try:
			aNewVault=self.layer2.create_vault(vault_name);
			print ("creating vault {} succeeded on glacier").format(vault_name)
			#updating local db	
			with glacier_shelve() as d:
				if not d.has_key("vaults"):
					d["vaults"]=dict()

				vaults = d["vaults"];
				
				if not vault_name in vaults:
					vaults[vault_name]=dict() #initialize a dictionary
				else:
					print ("a vault with the same name already exist in the local database")
				
				d["vaults"]=vaults

		except UnexpectedHTTPResponseError as inst:
			print inst.args
			expected_responses = inst.args
			print ("expected responses: {}").format(expected_responses)
			# print ("responses: {}").format(response)
			return
			
		
		
			


	def upload(self, filename):
		"""
		Upload filename and store the archive id for future retrieval
		"""
		if self.vault == None:
			print ("vault is not defined, use getVault first");
			return

		#this means vault exist
		print ("creating archieve and upload it to {}").format(self.vault.name)

		try:
			archive_id = self.vault.concurrent_create_archive_from_file(filename, description=filename)
			print ("successful transfer to the glacier, archive_id = {}").format(archive_id)
		except ArchiveError:
			print ("archieve error");
			return;
		except TreeHashDoesNotMatchError:
			print ("hash dismatch")
			return;
		except UploadArchiveError as inst:
			print ("upload archieve error");
			print inst;
			return;

		except UnexpectedHTTPResponseError:
			print ("unexcepted response")
			return;
 
		# Storing the filename => archive_id data.
		with glacier_shelve() as d:
			if not d.has_key("vaults"):
				d["vaults"] = dict()
		
			vaults=d["vaults"]

			if not self.vault.name in vaults:
				print ("{} is not in the local database").format(self.vault.name)
				return

			vault = vaults[self.vault.name]

			if not vault.has_key("archives"):
				vault["archives"] = dict()

			archives = vault["archives"]
			archives[filename] = archive_id

			#save it back
			vault["archives"] = archives
			vaults[self.vault.name]=vault
			d["vaults"]=vaults
			
	def resumeUpload(self,filename):
		#get fileID from local storage
		with glacier_shelve() as d:
			try:
				archives = d["vaults"][self.vault.name]["archives"]
			except:
				raise Exception("deleting from local database error")

			if filename in archives:
				print ("file {} found").format(filename)
				uploadID=archives[filename]
				print uploadID
				print "resuming upload"
				result = self.vault.resume_archive_from_file(uploadID, filename=filename, file_obj=None)
				print result

			else:
				print "file name does not exist in local db"


	def listJobs(self):
		if not self.vault:
			print ("Error: select vault by getVault first")
			return
		for job in self.vault.list_jobs():
			print "Job {action}: {status_code} ({creation_date}/{completion_date})".format(**job.__dict__)


	"""this read local database only"""
	def getArchiveId(self, file_name):
		"""
		Get the archive_id corresponding to the filename
		"""
		if not self.vault:
			print("Error: Please specify vault it is in with getVault first")
			return

		with glacier_shelve() as d:
			if not d.has_key("vaults"):
				d["vaults"] = dict()
 
			vaults = d["vaults"]

			if not self.vault.name in vaults:
				print("vault with current name does not have a copy in local database")
				return

			vault = vaults[self.vault.name]

			if not vault.has_key("archives"):
				vault["archives"] = dict()

			archives = vault["archives"]
			if file_name in archives:
				return archives[file_name]
			else:
				return None #can't find

	def retrieve(self, filename, wait_mode=False):
		"""
		Initiate a Job, check its status, and download the archive when it's completed.
		"""
		archive_id = self.getArchiveId(filename)
		print "archive_id found: {}".format(archive_id)
		if not archive_id:
			print "cannot find this file in local database, manually looking it up on glacier, this will take several hours"
			return
		
		with glacier_shelve() as d:
			if not d.has_key("jobs"):
				d["jobs"] = dict()
 
			jobs = d["jobs"]
			job = None
 
			if filename in jobs:
				# The job is already in shelve
				job_id = jobs[filename]
				try:
					job = self.vault.get_job(job_id)
				except UnexpectedHTTPResponseError: # Return a 404 if the job is no more available
					pass
 
			if not job:
				# Job initialization
				job = self.vault.retrieve_archive(archive_id)
				jobs[filename] = job.id
				job_id = job.id
 
			# Commiting changes in shelve
			d["jobs"] = jobs
 
		print "Job {action}: {status_code} ({creation_date}/{completion_date})".format(**job.__dict__)
 
		# checking manually if job is completed every 10 secondes instead of using Amazon SNS
		if wait_mode:
			import time
			while 1:
				job = self.vault.get_job(job_id)
				if not job.completed:
					time.sleep(10)
				else:
					break
 
		if job.completed:
			print "Downloading..."
			job.download_to_file(filename)
		else:
			print "Not completed yet"

	def delArchive(self,filename):
		if not self.vault:
			raise Exception("specify vault using getVault first")

		archive_id = self.getArchiveId(filename)
		if not archive_id:
			print ("no such filename exist");
			return
		
		print "deleting from galcier..."
		
		try:
			self.vault.delete_archive(archive_id);
		except UnexpectedHTTPResponseError:
			print "removing error"
			return
		
		print "removed from glacier"
		print "removing from local db"
		
		# Remove from database
		with glacier_shelve() as d:
			try:
				archives = d["vaults"][self.vault.name]["archives"]
			except:
				raise Exception("deleting from local database error")

			if filename in archives:
				del archives[filename]
				d["vaults"][self.vault.name]["archives"]=archives
				print "file removed from local db"
			else:
				print "file name does not exist in local db"

	def delVaultHelper(self,vault_name):
		#delete from glacier

		try:
			self.layer2.delete_vault(vault_name)
		except:
			print ("delete from the glacier error")

		#delete from local storage
		with glacier_shelve() as d:
			try:
				vaults = d["vaults"]
				if not vault_name in vaults:
					print ("Error: this vault is not in local database")
					return

				del vaults[vault_name]
				d["vaults"] = vaults
				print("Vault deleted from local DB")
				
			except:
				raise Exception("deleting from local database error")

		#at last remove clear current vault selection
		self.vault = None
		
	def delVault(self,vault_name):
		a_list_of_vaults = self.layer2.list_vaults();
		#check if the vault exist
		for vault in a_list_of_vaults:
			if vault.name == vault_name:
				self.delVaultHelper(vault_name) #vault exist in glacier, delete
				return
		raise Exception("vault with this name does not exist in Glacier")

	def printLocalDB(self):
		with glacier_shelve() as d:
			print (d)

	def debugVerifyPath(self,filename):
		print os.path.base(filename)



