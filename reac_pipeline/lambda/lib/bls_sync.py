import boto3
import requests
import hashlib
from bs4 import BeautifulSoup
from urllib.parse import urljoin

s3 = boto3.client("s3")

BLS_URL = "https://download.bls.gov/pub/time.series/pr/"
HEADERS = {
    "User-Agent": "Aarati-Engineer (aarati.dh222@gmail.com)"
}

def md5_checksum(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()

def list_s3_files(bucket: str, prefix: str) -> set:
    files = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            files.add(obj["Key"])
    return files

def list_bls_files(url):
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    soup = BeautifulSoup(res.content, "html.parser")

    results = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if not href or href in ("../", "./"):
            continue

        full_url = urljoin(url, href)
        if href.endswith("/"):
            results.extend(list_bls_files(full_url))
        else:
            relative_path = full_url.split("/pub/time.series/")[-1]
            results.append((relative_path, full_url))

    return results


def sync_bls_to_s3(bucket: str, prefix: str):
    bls_files = list_bls_files(BLS_URL)
    s3_files = list_s3_files(bucket, prefix)
    keep_keys = set()

    for relative_path, file_url in bls_files:
        key = prefix + relative_path
        keep_keys.add(key)

        resp = requests.get(file_url, headers=HEADERS)
        resp.raise_for_status()
        data = resp.content
        checksum = md5_checksum(data)

        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            if head["ETag"].strip('"') == checksum:
                continue
        except Exception:
            pass

        s3.put_object(Bucket=bucket, Key=key, Body=data)

    for key in s3_files:
        if key not in keep_keys:
            s3.delete_object(Bucket=bucket, Key=key)
