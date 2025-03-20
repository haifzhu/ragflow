#
#  Copyright 2025 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import oss2
import time
from io import BytesIO
from rag.utils import singleton
from rag import settings

@singleton
class RAGFlowOSS:
    def __init__(self):
        self.conn = None
        self.oss_config = settings.OSS
        self.access_key = self.oss_config.get('access_key', None)
        self.secret_key = self.oss_config.get('secret_key', None)
        self.endpoint_url = self.oss_config.get('endpoint_url', None)
        self.region = self.oss_config.get('region', None)
        self.bucket = self.oss_config.get('bucket', None)
        self.prefix_path = self.oss_config.get('prefix_path', None)
        self.__open__()
        self.create_bucket()
    
    def create_bucket(self):
        try:
            if not self.conn:
                return 
            if self.bucket_exists():
                return
            self.conn.create_bucket()
            logging.debug(f"create bucket {self.bucket} ********")
        except Exception:
            logging.exception(f"Fail to create bucket {self.bucket}")
    
    @staticmethod
    def use_prefix_path(method):
        def wrapper(self, bucket, fnm, *args, **kwargs):
            # If the prefix path is set, use the prefix path
            fnm = f"{bucket}/{fnm}" if bucket else fnm
            fnm = f"{self.prefix_path}/{fnm}" if self.prefix_path else fnm
            return method(self, self.bucket, fnm, *args, **kwargs)
        return wrapper

    def __open__(self):
        try:
            if self.conn:
                self.__close__()
        except Exception:
            pass

        try:
            self.conn = oss2.Bucket(
                oss2.Auth(self.access_key, self.secret_key), 
                self.endpoint_url, 
                self.bucket,
                self.region
            )
        except Exception:
            logging.exception(f"Fail to connect at region {self.region}")

    def __close__(self):
        del self.conn
        self.conn = None
    
    def bucket_exists(self):
        try:
            self.conn.get_bucket_info()
        except oss2.exceptions.NoSuchBucket:
            return False
        except:
            raise
        return True

    def health(self):
        fnm = "health"
        fnm, binary = f"{self.prefix_path}/{fnm}" if self.prefix_path else fnm, b"_t@@@1"
        r = self.conn.put_object(fnm, BytesIO(binary))
        return r

    def get_properties(self, bucket, key):
        return {}

    def list(self, bucket, dir, recursive=True):
        return []

    @use_prefix_path
    def put(self, bucket, fnm, binary):
        logging.debug(f"bucket name {bucket}; filename :{fnm}:")
        for _ in range(1):
            try:
                r = self.conn.put_object(fnm, BytesIO(binary))
                return r
            except Exception:
                logging.exception(f"Fail put {bucket}/{fnm}")
                self.__open__()
                time.sleep(1)

    @use_prefix_path
    def rm(self, bucket, fnm):
        try:
            self.conn.delete_object(fnm)
        except Exception:
            logging.exception(f"Fail rm {bucket}/{fnm}")

    @use_prefix_path
    def get(self, bucket, fnm):
        for _ in range(1):
            try:
                object_stream = self.conn.get_object(fnm)
                object_data = object_stream.read()
                return object_data
            except Exception:
                logging.exception(f"Fail get {bucket}/{fnm}")
                self.__open__()
                time.sleep(1)
        return

    @use_prefix_path
    def obj_exist(self, bucket, fnm):
        return self.conn.object_exists(fnm)

    @use_prefix_path
    def get_presigned_url(self, bucket, fnm, expires):
        for _ in range(10):
            try:
                r = self.conn.sign_url('GET', fnm, expires, slash_safe=True)
                return r
            except Exception:
                logging.exception(f"fail get url {bucket}/{fnm}")
                self.__open__()
                time.sleep(1)
        return

if __name__ == "__main__":
    instance = RAGFlowOSS()
    instance.health()