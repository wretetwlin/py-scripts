from git import Repo
import os

'''need to specify the absolute path for the repo directory'''
repo_dir = os.path.expanduser('~')+'/Desktop/py-scripts/py-scripts'
repo = Repo(repo_dir)
origin = repo.remote('origin')
origin.pull()
