import hashlib
import math
import sys


def log_error(format: str, *args):
    print(format % args, file=sys.stderr)


def getCookie(data):
    """
    通过加密对比得到正确cookie参数
    :param data: 参数
    :return: 返回正确cookie参数
    """
    chars = len(data["chars"])
    for i in range(chars):
        for j in range(chars):
            clearance = (
                data["bts"][0] + data["chars"][i] + data["chars"][j] + data["bts"][1]
            )
            encrypt = None
            if data["ha"] == "md5":
                encrypt = hashlib.md5()
            elif data["ha"] == "sha1":
                encrypt = hashlib.sha1()
            elif data["ha"] == "sha256":
                encrypt = hashlib.sha256()
            encrypt.update(clearance.encode())
            result = encrypt.hexdigest()
            if result == data["ct"]:
                return clearance


def getTotalPagesByTopTitle(soup, pageSize=10):
    return math.ceil(
        int(
            soup.find("div", attrs={"class": "top-title"})
            .find("span")
            .get_text()
            .replace(",", "")
        )
        / pageSize
    )
