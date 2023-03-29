# -*- coding: utf-8 -*-

"""
MongoDBクライアントアプリケーションを提供するモジュール.
"""
import os
from enum import Enum
from typing import List
from pymongo import MongoClient
from pymongo.database import Database


class CommandName(Enum):
    """MongoDBのコマンド名の列挙型クラスです。
    """
    REPLICA_SET_STATUS = "replSetGetStatus"
    DB_STATS = "dbstats"
    COLLECTION_STATS = "collstats"


class MongoDbClient(object):
    """MongoDBクライアントを表すクラスです。
    """
    def __init__(self,
                 host: str = "localhost",
                 port: int = 27017,
                 user: str = "admin",
                 password: str = None,
                 db_name: str = "admin"):
        """このクラスのオブジェクトを初期化します.
        :param host: ホスト名orIPアドレス
        :param port: ポート番号
        :param user: ログインユーザ名
        :param password: ログインパスワード
        :param db_name: データベース名
        """
        self.host: str = os.getenv("MGHOST", host)
        self.port: int = int(os.getenv("MGPORT", port))
        self.user: str = os.getenv("MGUSER", user)
        self.password: str = os.getenv("MGPASSWORD", password)
        self.db_name: str = os.getenv("MGDATABASE", db_name)
        self.client = MongoClient(host=self.host,
                                  username=self.user,
                                  password=self.password,
                                  authSource=self.db_name,
                                  port=int(self.port))

    def get_primary_info(self) -> dict:
        """MongoDBのプライマリー情報を取得します。
        :return: MongoDBのプライマリー情報
        """
        db = self.get_db()
        rep_status = db.command(CommandName.REPLICA_SET_STATUS.value)
        members = rep_status.get('members')
        for member in members:
            if member.get('stateStr') == 'PRIMARY':
                return member

    def get_db(self, db_name: str = None) -> Database:
        """データベースを取得します。
        :param db_name: データベース名
        :return: データベース
        """
        if db_name is None:
            return self.client[self.db_name]
        else:
            return self.client[db_name]

    def get_db_stats(self, db_name: str = None) -> dict:
        """データベースのステータスを取得します。
        :param db_name: データベース名
        :return: データベースのステータス
        """
        db = self.get_db(db_name)
        return db.command(CommandName.DB_STATS.value)

    def get_collection_stats(self, db_name: str = None) -> List[dict]:
        """データベースのコレクションのステータスを取得します。
        :param db_name: データベース名
        :return: コレクションのステータスリスト
        """
        stats = list()
        db = self.get_db(db_name)
        collection_names = db.list_collection_names()
        for collection_name in collection_names:
            collection_stats = db.command({CommandName.COLLECTION_STATS.value: collection_name})
            stats.append(collection_stats)
        return stats

    def __enter__(self):
        """withブロックに入る時に実行する処理です.
        このクラスのオブジェクトを返却します.
        :return:
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """withブロックから出る時に実行する処理です.
        このDB接続をクローズします.
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.close()

    def close(self):
        """このDB接続をクローズします.
        :return:
        """
        if self.client is not None:
            self.client.close()
