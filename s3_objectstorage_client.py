# -*- coding: utf-8 -*-
import os
from botocore.client import Config
from boto3 import Session
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)


class S3Client(object):
    """
    オブジェクトストレージ（S3互換）のクライアントクラス
    """

    def __init__(self):
        """
        初期処理。各種連携定義を環境変数から取得後、S3オブジェクトクライアント生成・接続を実施。
        """
        # 環境変数設定
        self._service = os.getenv('S3_SERVICE', "s3")
        self._region = os.getenv('REGION', "")
        self._endpoint = os.getenv('OBJECTSTORAGE_S_SERVICE_ENDPOINT', "")
        self._access_key = os.getenv('S3_ACCESS_KEY', "")
        self._secret_key = os.getenv('S3_SECRET_KEY', "")
        self._request_retries = int(os.getenv('S3_REQUEST_RETRIES', "5"))
        self._sign_version = os.getenv('S3_SIGN_VERSION', "s3v4")

        # S3resourceを生成
        self._s3_resource()

    def _s3_resource(self):
        """
        オブジェクトストレージ（S3互換）リソースを生成する。
        """
        # リソースサービスクライアントを作成
        self._s3_resource = Session().resource(
            self._service,
            region_name=self._region,
            endpoint_url=self._endpoint,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            verify=False,
            config=Config(
                signature_version=self._sign_version,
                retries=dict(max_attempts=self._request_retries)
            )
        )

    def s3_client(self):
        """
        オブジェクトストレージ（S3互換）クライアントを取得する。
        :return: オブジェクトストレージ（S3互換）オブジェクト
        """
        return self._s3_resource.meta.client

    def s3_bucket(self, bucket_name):
        """
        バケットを取得する。
        :return: ログサービスのバケットオブジェクト
        """
        return self._s3_resource.Bucket(bucket_name)

    def upload_file(self, file_name, bucket_name, s3_path):
        """
        バケットにファイルをアップロードする
        :param file_name: ローカルに格納しているファイル名(フルパス)
        :param bucket_name: アップロード先のバケット名
        :param s3_path: アップロード先のファイル名(フルパス)
        """
        self.s3_bucket(bucket_name).upload_file(file_name, s3_path)

    def copy_object(self, source_bucket_name, source_object_path, target_bucket_name, target_object_path):
        """
        オブジェクトをコピーする
        :param source_bucket_name: コピー元のバケット名
        :param source_object_path: コピー元のオブジェクト名
        :param target_bucket_name: コピー先のバケット名
        :param target_object_path: コピー先のオブジェクト名
        """
        # コピー元のバケットの情報を設定しておく
        copy_source = {'Bucket': source_bucket_name, 'Key': source_object_path}
        # コピー先のバケット、オブジェクトを取得しておく
        target_bucket = self._s3_resource.Bucket(target_bucket_name)
        target_object = target_bucket.Object(target_object_path)
        # コピー先のオブジェクトに対して、コピー元の情報を設定し、コピーする。
        target_object.copy(copy_source)

    def delete_object(self, bucket_name, object_key):
        """
        オブジェクトを削除する
        :param bucket_name: オブジェクトが格納されているバケット名
        :param object_key: 削除するオブジェクト名
        """
        self.s3_client().delete_object(Bucket=bucket_name, Key=object_key)
