from enum import Enum
import lz4
import lzma
import os
import sqlite3
import tempfile
import zstandard
from typing import Any, Dict

from dataclasses import dataclass

import lz4.frame


class CompressionType(Enum):
    LZMA = "LZMA"
    LZ4 = "LZ4"
    ZSTD = "ZSTD"


@dataclass
class S4AEntryMetadata:
    name: str
    _type: str
    offset: int
    size: int
    compresssion: str


def parse_s4a_db(db_file):
    try:
        conn = sqlite3.Connection(db_file)
        cur = conn.cursor()
        res_iter = cur.execute("SELECT name, type, offset, size, compression FROM entry_list")
        entry_list = res_iter.fetchall()
        return {x[0]: S4AEntryMetadata(x[0], x[1], x[2], x[3], x[4]) for x in entry_list}
    except Exception as e:
        print(f"error parsing s4a db: {e}")
        return None


@dataclass
class S4AReaderS3:
    s3_client: Any
    bucket_name: str
    blob_key_name: str
    blob_offset: int
    entry_map: Dict[str, S4AEntryMetadata]

    def get_file(self, name: str):
        if name in self.entry_map:
            entry_info = self.entry_map[name]
            try:
                s3_get_resp = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=self.blob_key_name,
                    Range=f"bytes={entry_info.offset}-{entry_info.offset + entry_info.size - 1}"
                )
                s3_object_data = s3_get_resp['Body'].read()
            except Exception as e:
                print(f"error getting file from blob: {e}")
                return None
            try:
                uncompressed_data = lzma.decompress(s3_object_data)
            except Exception as e:
                print(f"error un-compressing data: {e}")
                return None
            return uncompressed_data


def make_s4a_reader_s3(s3_client: Any, bucket_name: str, object_name: str):
    try:
        s3_get_db_size = s3_client.get_object(
            Bucket=bucket_name,
            Key=object_name,
            Range=f"bytes=0-7"
        )
        db_size = s3_get_db_size['Body'].read()
    except Exception as e:
        print(f"ERROR reading {object_name} in {bucket_name} from S3: {e}")
        return None
    db_size_int = int.from_bytes(db_size, byteorder='big')
    try:
        s3_get_db_data = s3_client.get_object(
            Bucket=bucket_name,
            Key=object_name,
            Range=f"bytes=8-{db_size_int - 1}"
        )
        db_data = s3_get_db_data['Body'].read()
    except Exception as e:
        print(f"ERROR reading {object_name} in {bucket_name} from S3: {e}")
        return None
    temp_file = tempfile.NamedTemporaryFile()
    temp_file.write(db_data)
    try:
        entry_map = parse_s4a_db(temp_file.name)
    except Exception as e:
        print(f"error parsing .s4a.db: {e}")
        return None
    return S4AReaderS3(s3_client, bucket_name, object_name, entry_map)


@dataclass
class S4AFileReaderLocal:
    blob_path: str
    blob_offset: int
    entry_info: S4AEntryMetadata

    def read(self):
        try:
            with open(self.blob_path, 'rb') as fr:
                fr.seek(self.entry_info.offset + self.blob_offset)
                compressed_data = fr.read(self.entry_info.size)
        except Exception as e:
            print(f"error getting file from blob: {e}")
            return None
        try:
            if self.entry_info.compresssion == "LZMA":
                uncompressed_data = lzma.decompress(compressed_data)
            elif self.entry_info.compresssion == "LZ4":
                uncompressed_data = lz4.frame.decompress(compressed_data)
            elif self.entry_info.compresssion == "ZSTD":
                uncompressed_data = zstandard.ZstdDecompressor().decompress(compressed_data)
            else:
                print(f"error: unknown compression type: {self.entry_info.compresssion}")
                return None
        except Exception as e:
            print(f"error un-compressing data: {e}")
            return None
        return uncompressed_data


@dataclass
class S4AReaderLocal:
    blob_path: str
    blob_offset: int
    entry_map: Dict[str, S4AEntryMetadata]

    def get_file(self, name: str):
        entry_reader = self.get_file_reader(name) 
        if entry_reader is not None:
            return entry_reader.read()
        print(f"can't find {name} in archive")
        return None


    def get_file_reader(self, name: str):
        if name in self.entry_map:
            return S4AFileReaderLocal(
                blob_path=self.blob_path,
                blob_offset=self.blob_offset,
                entry_info=self.entry_map[name],
            )
        print(f"can't find {name} in archive")
        return None


def make_s4a_reader_local(archive_path: str):
    if archive_path.endswith(".s4a"):
        db_size = bytearray()
        try:
            fr = open(archive_path, 'rb')
        except Exception as e:
            print(f"error opening .s4a: {e}")
            return None
        with fr:
            db_size.extend(fr.read(8))
            db_size_int = int.from_bytes(db_size, byteorder='big')
            db_data = fr.read(db_size_int)
            db_data_uncompressed = lzma.decompress(db_data)
            tempfile_obj = tempfile.NamedTemporaryFile()
            tempfile_obj.write(db_data_uncompressed)

        try:
            entry_map = parse_s4a_db(tempfile_obj.name)
        except Exception as e:
            print(f"error parsing .s4a: {e}")
            return None
        return S4AReaderLocal(archive_path, db_size_int + 8, entry_map)
    elif archive_path.endswith(".s4a.db"):
        try:
            entry_map = parse_s4a_db(archive_path)
        except Exception as e:
            print(f"error parsing .s4a.db: {e}")
            return None
        return S4AReaderLocal(archive_path[:-3] + ".blob", 0, entry_map)
    else:
        return None


if __name__ == "__main__":
    reader = make_s4a_reader_local("/Users/sadigopu/vscodeproj/s3-seek-archive/rust-compressor/ex_arc.s4a.db")
    print(reader.entry_map)
    print(reader.get_file("CACHEDIR.TAG").decode())
    pass