from enum import Enum
import lzma
import sqlite3
import tempfile
from typing import Any, Dict

from dataclasses import dataclass


class CompressionType(Enum):
    LZMA = "LZMA"
    LZ4 = "LZ4"


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
class S4AReaderLocal:
    blob_path: str
    blob_offset: int
    entry_map: Dict[str, S4AEntryMetadata]

    def get_file(self, name: str):
        if name in self.entry_map:
            entry_info = self.entry_map[name]
            try:
                with open(self.blob_path, 'rb') as fr:
                    fr.seek(entry_info.offset + self.blob_offset)
                    compressed_data = fr.read(entry_info.size)
            except Exception as e:
                print(f"error getting file from blob: {e}")
                return None
            try:
                uncompressed_data = lzma.decompress(compressed_data)
            except Exception as e:
                print(f"error un-compressing data: {e}")
                return None
            return uncompressed_data


def make_s4a_reader_local(s4a_path: str):
    db_size = bytearray()
    try:
        fr = open(s4a_path, 'rb')
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
    return S4AReaderLocal(s4a_path, db_size_int + 8, entry_map)

if __name__ == "__main__":
    reader = make_s4a_reader_local("/Users/sadigopu/RustroverProjects/s3-seek-archive/rust-compressor/target_archive.s4a")
    print(reader.entry_map)
    pass