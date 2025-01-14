from tqdm import tqdm
import requests
import os

class DataPull:

    def __init__(self, saving_dir:str="data/", debug:bool=False) -> None:

        self.debug = debug
        self.saving_dir = saving_dir

        # Check if the saving directory exists
        if not os.path.exists(self.saving_dir + "raw"):
            os.makedirs(self.saving_dir + "raw")
        if not os.path.exists(self.saving_dir + "processed"):
            os.makedirs(self.saving_dir + "processed")
        if not os.path.exists(self.saving_dir + "external"):
            os.makedirs(self.saving_dir + "external")

        if not os.path.exists(f"{self.saving_dir}external/cousub.zip"):
            self.pull_file(url="https://www2.census.gov/geo/tiger/TIGER2024/COUSUB/tl_2024_72_cousub.zip", filename=f"{self.saving_dir}external/cousub.zip")
            self.debug_log("county.zip downloaded")


    def pull_file(self, url:str, filename:str, verify:bool=True) -> None:
        """
        Pulls a file from a URL and saves it in the filename. Used by the class to pull external files.

        Parameters
        ----------
        url: str
            The URL to pull the file from.
        filename: str
            The filename to save the file to.
        verify: bool
            If True, verifies the SSL certificate. If False, does not verify the SSL certificate.

        Returns
        -------
        None
        """
        chunk_size = 10 * 1024 * 1024

        with requests.get(url, stream=True, verify=verify) as response:
            total_size = int(response.headers.get('content-length', 0))

            with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024, desc='Downloading') as bar:
                with open(filename, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            file.write(chunk)
                            bar.update(len(chunk))  # Update the progress bar with the size of the chunk

    def debug_log(self, message:str) -> None:
        if self.debug:
            print(f"\033[0;36mINFO: \033[0m {message}")
