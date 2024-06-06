import argparse
import requests
import io
import zipfile
import os
import shutil

class PublicReposManager:

    def __init__(self, owner):
        self.owner = owner
        self.error = None

    def getPublicRepo(self, repo_name, folder_name, tag, /, *, customURL = None):
        headers = {'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'}
        url = f'https://api.github.com/repos/{self.owner}/{repo_name}/zipball/{tag}'
        if customURL is not None:
            url = customURL
        download_dir = f'{os.curdir}/{folder_name}'
        r = requests.get(url, headers=headers)
        if not r.ok:
            self.error = f'\n{r.status_code}: {r.reason}. \nURL={url}'
        r.raise_for_status()
        if os.path.exists(download_dir):
            shutil.rmtree(download_dir)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        temp, = zipfile.Path(z).iterdir()
        z.extractall(path=f'{os.curdir}/')
        z.close()
        os.rename(f'{os.curdir}/{temp.name}', download_dir)

def main():
    parser = argparse.ArgumentParser(description='Download scripts for building project.')
    parser.add_argument('owner', help='github owner')
    parser.add_argument('download_folder_name', help='location for downloading scripts')
    args = parser.parse_args()
    reposManager = PublicReposManager(args.owner)
    reposManager.getPublicRepo('CMakeBuildScripts', args.download_folder_name, 'master')
    if reposManager.error is not None:
        print(reposManager.error)
    else:
        print('Done.')

if __name__ == "__main__":
    main()