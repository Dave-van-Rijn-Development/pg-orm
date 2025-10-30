from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from Crypto.Util.Padding import unpad
import base64


class Encryption:
    _secret: bytes = b''

    @staticmethod
    def configure(*, secret: str):
        Encryption._secret = secret.encode()

    def __init__(self):
        self._cipher = AES.new(self._secret, AES.MODE_ECB)

    def encrypt(self, value: str) -> str:
        if not value:
            return ''
        ciphertext = self._cipher.encrypt(pad(value.encode("utf-8"), AES.block_size))
        data_b64 = base64.b64encode(ciphertext)
        return data_b64.decode('utf-8')

    def decrypt(self, value: str) -> str:
        if not value:
            return ''
        ciphertext = base64.decodebytes(value.encode('utf-8'))
        decrypted = unpad(self._cipher.decrypt(ciphertext), AES.block_size)
        return decrypted.decode('utf-8')


def encrypt(value: str, salt: int = None) -> str:
    return _get_encryption().encrypt(value)


def decrypt(value: str) -> str:
    return _get_encryption().decrypt(value)


_encryption: Encryption | None = None


def _get_encryption() -> Encryption:
    global _encryption
    if _encryption is None:
        _encryption = Encryption()
    return _encryption
