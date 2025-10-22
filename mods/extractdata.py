"""
各種提取資料做轉化的函式庫
Creator: Chgwyellow

from mods import extractdata as ed
"""

import re


def extract_city_district(address: str) -> tuple[str, str]:
    """從機構的地址取出所在市與區
    此處re的pattern是設定為六都及轄下區域

    Args:
        address (str): 要尋找的地址

    Returns:
        tuple[str, str]: 前者返回city, 後者返回district, 如果沒有則都返回None
    """

    # 這裡pattern用六都的方式做設定
    pattern = r"(臺北市|台北市|新北市|桃園市|台中市|台南市|高雄市)(.*?區)"
    match = re.search(pattern=pattern, string=address)
    if match:
        return match.group(1), match.group(2)
    return None, None
