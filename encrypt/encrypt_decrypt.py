import base64
import os

from cryptography.fernet import Fernet  # symmetric encryption
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class AesEncrypt:
    def __init__(self, private_key='rnstntk'):
        self.private_key = private_key

        password = private_key.encode()
        salt = os.urandom(16)
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000, backend=default_backend())
        key = base64.urlsafe_b64encode(kdf.derive(password))

        self.f = Fernet(key)

    def encrypt(self, data, is_out_string=True):
        if isinstance(data, bytes):
            ou = self.f.encrypt(data)  # 바이트형태이면 바로 암호화
        else:
            ou = self.f.encrypt(data.encode('utf-8'))  # 인코딩 후 암호화
        if is_out_string is True:
            return ou.decode('utf-8')  # 출력이 문자열이면 디코딩 후 반환
        else:
            return ou

    def decrypt(self, data, pw, is_out_string=True):
        if pw == self.private_key:
            if isinstance(data, bytes):
                ou = self.f.decrypt(data)  # 바이트형태이면 바로 복호화
            else:
                # ttl: the number of seconds old a message may be for it to be valid.
                # If the message is older than ttl seconds (from the time it was originally created)
                # an exception will be raised.
                ou = self.f.decrypt(data.encode('utf-8'), ttl=300)  # 인코딩 후 복호화
            if is_out_string is True:
                return ou.decode('utf-8')  # 출력이 문자열이면 디코딩 후 반환
            else:
                return ou
        else:
            return 'Permission Denied'
