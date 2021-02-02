#!/bin/python3

import os
import time
from typing import Optional, Tuple

import requests
import shutil
import gzip

import pandas as pd

from requests import Response

download_images: bool = True
download_videos: bool = False

video_listings: str = "https://parler.ddosecrets.com/static/ddosecrets-parler-listing.txt.gz"
image_listings: str = "https://parler.ddosecrets.com/static/ddosecrets-parler-images-listing.txt.gz"

videos_file_regex: str = r"^(\w{2})(\w{2})"
images_file_regex: str = r"^(\w{1})(\w{1})"

# https://web.archive.org/web/20210110201435im_/https://image-cdn.parler.com/z/4/z4cxIMCHHx.jpeg
videos_url_format: str = r"https://web.archive.org/web/20210111044311id_/https://video.parler.com/\1/\2/\1\2"
images_url_format: str = r"https://web.archive.org/web/20210110201435im_/https://image-cdn.parler.com/\1/\2/\1\2"

working_dir: str = "working"
listings_dir: str = os.path.join(working_dir, "listings")

video_list_archive: str = os.path.join(listings_dir, "video-listing.txt.gz")
image_list_archive: str = os.path.join(listings_dir, "image-listing.txt.gz")

video_list_path: str = os.path.join(listings_dir, "video-listing.txt")
image_list_path: str = os.path.join(listings_dir, "image-listing.txt")

video_list_csv: str = os.path.join(listings_dir, "video-listing.csv")
image_list_csv: str = os.path.join(listings_dir, "image-listing.csv")

video_download_path: str = os.path.join(working_dir, "videos")
image_download_path: str = os.path.join(working_dir, "images")


def get_dataframe(list_path: str) -> pd.DataFrame:
    data_list: pd.DataFrame = pd.read_csv(filepath_or_buffer=list_path, delim_whitespace=True, header=None,
                                          usecols=[2, 3])
    return data_list.rename(columns={2: "size", 3: "file"}, inplace=False)


# Stolen From: https://stackoverflow.com/a/52333182
def gunzip_shutil(source_filepath, dest_filepath, block_size=65536):
    with gzip.open(source_filepath, 'rb') as s_file, open(dest_filepath, 'wb') as d_file:
        shutil.copyfileobj(s_file, d_file, block_size)


def setup() -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not os.path.exists(working_dir):
        print("Creating Working Directory!!!")
        os.mkdir(working_dir)

    if not os.path.exists(listings_dir):
        print("Creating Listings Directory!!!")
        os.mkdir(listings_dir)

    if not os.path.exists(video_list_archive):
        print("Downloading Video Listings!!!")
        video_list_resp: Response = requests.get(url=video_listings, stream=True)

        with open(file=video_list_archive, mode="wb") as f:
            shutil.copyfileobj(video_list_resp.raw, f)
        del video_list_resp

    if not os.path.exists(image_list_archive):
        print("Downloading Image Listings!!!")
        image_list_resp: Response = requests.get(url=image_listings, stream=True)

        with open(file=image_list_archive, mode="wb") as f:
            shutil.copyfileobj(image_list_resp.raw, f)
        del image_list_resp

    if not os.path.exists(video_download_path):
        print("Creating Video Download Folder!!!")
        os.mkdir(video_download_path)

    if not os.path.exists(image_download_path):
        print("Creating Image Download Folder!!!")
        os.mkdir(image_download_path)

    if not os.path.exists(video_list_path):
        print("Extracting Video List!!!")
        gunzip_shutil(source_filepath=video_list_archive, dest_filepath=video_list_path)

    if not os.path.exists(image_list_path):
        print("Extracting Image List!!!")
        gunzip_shutil(source_filepath=image_list_archive, dest_filepath=image_list_path)

    if not os.path.exists(video_list_csv):
        print("Creating Video DataFrame!!!")
        video_list_df: pd.DataFrame = get_dataframe(list_path=video_list_path)
        video_list_df["url"] = video_list_df["file"].replace(to_replace=videos_file_regex, value=videos_url_format,
                                                             inplace=False, regex=True)

        video_list_df.to_csv(path_or_buf=video_list_csv, index=False)
    else:
        print("Loading Video DataFrame!!!")
        video_list_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=video_list_csv)

    if not os.path.exists(image_list_csv):
        print("Creating Image DataFrame!!!")
        image_list_df: pd.DataFrame = get_dataframe(list_path=image_list_path)
        image_list_df["url"] = image_list_df["file"].replace(to_replace=images_file_regex, value=images_url_format,
                                                             inplace=False, regex=True)

        image_list_df.to_csv(path_or_buf=image_list_csv, index=False)
    else:
        print("Loading Image DataFrame!!!")
        image_list_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=image_list_csv)

    return video_list_df, image_list_df


def download_media(media_urls: pd.DataFrame, download_directory: str):
    total: int = len(media_urls)
    for row in media_urls.itertuples():
        file_path: str = os.path.join(download_directory, row.file)
        if os.path.exists(file_path):
            print(f"Skipping {row.file}")
            continue

        print(f"{row.Index}/{total} Downloading {row.file}")
        image_resp: Response = requests.get(url=row.url, stream=True)

        with open(file=file_path, mode="wb") as f:
            shutil.copyfileobj(image_resp.raw, f)

        print("Sleeping 5 Seconds")
        time.sleep(5)


video_list, image_list = setup()

print(video_list)
print(image_list)

if download_images:
    print("Downloading Images!!!")
    download_media(media_urls=image_list, download_directory=image_download_path)

if download_videos:
    print("Downloading Videos!!!")
    download_media(media_urls=video_list, download_directory=video_download_path)