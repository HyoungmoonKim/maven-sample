# -*- coding: utf-8 -*-
import os
from pathlib import Path

from pyzipper import ZIP_DEFLATED, WZ_AES, AESZipFile
from hashlib import sha512


class ZipUtil:
    """パスワード付zipファイルの生成・解凍ユティリティ
    """
    # エンコード文字列
    utf_8 = 'utf-8'
    # パスワードハッシュ化で付与する特定文字列
    append_str = 'append-pwd'

    @staticmethod
    def compress(zip_file: Path, src_dir: Path, output_dir: str):
        """
        パスワード付のZipファイルを生成する
        :param zip_file: zipファイル
        :param src_dir: 圧縮対象となるディレクトリ
        :param output_dir: 圧縮ファイルの中に作成するディレクトリ名
        """
        with AESZipFile(zip_file, mode='w', compression=ZIP_DEFLATED, encryption=WZ_AES) as z_file:
            # パスワード設定
            pwd = ZipUtil.hashed(zip_file).encode(ZipUtil.utf_8)
            z_file.setpassword(pwd)
            # 指定ディレクトリ配下のすべてのファイルを圧縮
            contents = os.walk(src_dir)
            for src_dir, sub_dir, files in contents:
                z_file.write(src_dir, arcname=output_dir)
                for file in files:
                    z_file.write(os.path.join(src_dir, file), arcname=f'{output_dir}/{file}')

    @staticmethod
    def uncompress(zip_file: Path, path: Path):
        """
        パスワード付のZipファイルを解凍する
        :param zip_file: zipファイル
        :param path: 展開先ディレクトリ
        """
        with AESZipFile(zip_file, mode='r') as z_file:
            # パスワード設定
            pwd = ZipUtil.hashed(zip_file).encode(ZipUtil.utf_8)
            z_file.setpassword(pwd)
            # 展開
            z_file.extractall(path=path)

    @staticmethod
    def hashed(file: Path) -> str:
        """
        ハッシュコードを生成する
        :param file: ファイル
        :return:ハッシュコード
        """
        # 圧縮ファイル名に固定の文字列を加えてハッシュ値を生成
        hashed_str = file.name + ZipUtil.append_str
        return sha512(hashed_str.encode(ZipUtil.utf_8)).hexdigest()
