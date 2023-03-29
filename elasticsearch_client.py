# -*- coding: utf-8 -*-

"""
Elasticsearchのクライアントアプリケーションを提供するモジュール.
"""
import os
from elasticsearch import Elasticsearch, RequestsHttpConnection
from typing import List, Dict
import warnings
from elasticsearch.exceptions import ElasticsearchWarning


class ElasticsearchConnection(RequestsHttpConnection):
    """プロキシ経由でのElasticsearch接続が可能なTransportクラスです.
    """
    def __init__(self, *args, **kwargs):
        """このクラスのオブジェクトを初期化します.

        :params *args: 可変長パラメータ(Tuple)
        :params **kwargs: 可変長パラメータ(Dict).
        """
        proxies = kwargs.pop('proxies', {})
        super(ElasticsearchConnection, self).__init__(*args, **kwargs)
        self.session.proxies = proxies


class ElasticsearchClient:
    """Elasticsearch-APIsを実行するクライアントクラスです.
    """
    def __init__(self,
                 host_ports: List = None,
                 user: str = None,
                 password: str = None,
                 use_ssl: bool = True):
        """このクラスのオブジェクトを初期化します.

        :params host_ports: 接続先のホスト名(IPアドレス)とポート番号のリスト.ex:{hostname:port}
        :params user: ユーザ.
        :params password: パスワード.
        :params use_ssl: 接続にSSLを使用する場合はTrue,使用しない場合はFalse.(省略時はTrue)
        """
        self.__host_ports = host_ports
        host_ports_str = os.getenv("ES_HOSTS")
        if host_ports_str is not None:
            self.__host_ports = host_ports_str.split(',')
        self.__user = os.getenv("ES_USER", user)
        self.__password = os.getenv("ES_PASSWD", password)
        self.__timeout = os.getenv("ES_TIMEOUT", 60)
        self.__max_retries = os.getenv("ES_MAX_RETRIES", 5)
        self.__use_ssl = use_ssl
        proxies = dict()
        http_proxy = os.getenv("HTTP_PROXY")
        https_proxy = os.getenv("HTTPS_PROXY")
        if http_proxy is not None:
            proxies["http"] = http_proxy
        if https_proxy is not None:
            proxies["https"] = https_proxy
        if not self.__use_ssl:
            self._session = Elasticsearch(
                self.__host_ports,
                http_auth=(self.__user, self.__password),
                use_ssl=self.__use_ssl,
                proxies=proxies,
                connection_class=ElasticsearchConnection,
                timeout=self.__timeout,
                max_retries=self.__max_retries,
                retry_on_timeout=True,
            )
        else:
            self._session = Elasticsearch(
                self.__host_ports,
                http_auth=(self.__user, self.__password),
                use_ssl=self.__use_ssl,
                verify_certs=False,
                ssl_show_warn=False,
                proxies=proxies,
                connection_class=ElasticsearchConnection,
                timeout=self.__timeout,
                max_retries=self.__max_retries,
                retry_on_timeout=True,
            )

    @property
    def host_ports(self) -> List:
        """接続先のホスト名(IPアドレス)とポート番号のリストです.

        :return: 接続先のホスト名(IPアドレス)とポート番号のリスト.
        """
        return self.__host_ports

    @property
    def user(self) -> str:
        """HTTP認証のユーザ名です.

        :return: HTTP認証のユーザ名.
        """
        return self.__user

    @property
    def password(self) -> str:
        """HTTP認証のパスワードです.

        :return: HTTP認証のパスワード.
        """
        return self.__password

    def __enter__(self):
        """withブロックに入る時に実行する処理です.
        このクラスのオブジェクトを返却します.

        :return: このクラスのオブジェクト.
        """
        return self

    def __exit__(self, ex_type, ex_value, trace):
        """withブロックから出る時に実行する処理です.
        このセッションをクローズします.

        :param ex_type:
        :param ex_value:
        :param trace:
        """
        self.close()

    def close(self):
        """このセッションをクローズします.

        """
        self._session.transport.close()
        # self._session.close()

    def search(self,
               index: str = None,
               query: Dict = None,
               source_includes: List = None,
               source_excludes: List = None,
               scroll: str = "5m",
               size: int = 1000,
               timeout: float = None) -> List:
        """指定されたクエリDSLを実行し、クエリに一致する検索ヒットを返却します.

        :param index: 検索するindex名.(複数の場合はカンマで区切って指定)
        :param query: クエリDSL(検索定義)
        :param source_includes: `_source`フィールドに含めるKeyのリスト.
        :param source_excludes: `_source`フィールドから除外するKeyのリスト.
        :param scroll: スクロール検索でindexの一貫したビューを維持する期間(省略時は5m)
        :param size: 1スクロール当りの検索hit数(省略時は1000)
        :param timeout: リクエストタイムアウト(省略時はタイムアウト無し)
        :return: クエリに一致した検索ヒット.
        """

        # [The client is unable to verify that the server is Elasticsearch due security privileges on the server side]
        # の警告メッセージを抑止
        warnings.simplefilter("ignore", ElasticsearchWarning)

        # 環境変数を設定していた場合は,環境変数の値を設定し,
        # 環境変数を設定していなかった場合は,パラメータの値を設定
        scroll = os.getenv("ES_DEFAULT_SEARCH_SCROLL", scroll)
        size = int(os.getenv("ES_DEFAULT_SEARCH_SIZE", size))

        # [GET `_search`]を実行
        data = self._session.search(
            index=index,
            doc_type=None,
            scroll=scroll,
            size=size,
            query=query,
            _source_includes=source_includes,
            _source_excludes=source_excludes,
            request_timeout=timeout
        )

        # クエリに一致した検索ヒットを生成
        result = list()
        scroll_hits = data["hits"]["hits"]
        scroll_size = len(scroll_hits)
        result.extend(scroll_hits)

        # スクロールが存在する場合は,
        # 全てスクロールするまで[POST /_search/scroll]を実行
        scroll_id = data["_scroll_id"]
        total_size = data["hits"]["total"]["value"]
        scrolled_size = scroll_size
        while total_size > scrolled_size:
            data = self._session.scroll(scroll_id=scroll_id, scroll=scroll)
            scroll_hits = data["hits"]["hits"]
            result.extend(scroll_hits)
            scroll_size = len(scroll_hits)
            scrolled_size += scroll_size

        # [DELETE /_search/scroll]を実行
        if scroll_id is not None:
            self._session.clear_scroll(scroll_id=scroll_id)

        # クエリに一致した検索ヒットを返却
        return result

    def count(self, index: str = None, query: Dict = None, timeout: float = None) -> Dict:
        """指定されたクエリDSLを実行し、クエリに一致するドキュメント数を返却します.

        :param index: 検索するindex名.(複数の場合はカンマで区切って指定)
        :param query: クエリDSL(検索定義)
        :param timeout: リクエストタイムアウト(省略時はタイムアウト無し)
        :return: クエリに一致したドキュメント数.
        """

        # [The client is unable to verify that the server is Elasticsearch due security privileges on the server side]
        # の警告メッセージを抑止
        warnings.simplefilter("ignore", ElasticsearchWarning)

        # クエリDSLが指定された場合は,リクエストBODYを生成
        body = None
        if query is not None:
            body = {"query": query}

        # [GET /_count]を実行
        result = self._session.count(index=index, body=body, request_timeout=timeout)

        # クエリに一致したドキュメント数を返却
        return result
