"""
都道府県順序定義
Business Data Processor

総務省の全国地方公共団体コード（JIS X 0401）順
北海道(01) → 沖縄県(47)
"""

# 47都道府県の順序定義（北から南）
PREFECTURE_ORDER = [
    "北海道",   # 0
    "青森県",   # 1
    "岩手県",   # 2
    "宮城県",   # 3
    "秋田県",   # 4
    "山形県",   # 5
    "福島県",   # 6
    "茨城県",   # 7
    "栃木県",   # 8
    "群馬県",   # 9
    "埼玉県",   # 10
    "千葉県",   # 11
    "東京都",   # 12
    "神奈川県", # 13
    "新潟県",   # 14
    "富山県",   # 15
    "石川県",   # 16
    "福井県",   # 17
    "山梨県",   # 18
    "長野県",   # 19
    "岐阜県",   # 20
    "静岡県",   # 21
    "愛知県",   # 22
    "三重県",   # 23
    "滋賀県",   # 24
    "京都府",   # 25
    "大阪府",   # 26
    "兵庫県",   # 27
    "奈良県",   # 28
    "和歌山県", # 29
    "鳥取県",   # 30
    "島根県",   # 31
    "岡山県",   # 32
    "広島県",   # 33
    "山口県",   # 34
    "徳島県",   # 35
    "香川県",   # 36
    "愛媛県",   # 37
    "高知県",   # 38
    "福岡県",   # 39
    "佐賀県",   # 40
    "長崎県",   # 41
    "熊本県",   # 42
    "大分県",   # 43
    "宮崎県",   # 44
    "鹿児島県", # 45
    "沖縄県",   # 46
]


def get_prefecture_order(prefecture_name: str) -> int:
    """
    都道府県名から順序番号を取得

    Args:
        prefecture_name: 都道府県名

    Returns:
        int: 順序番号（0-46）、見つからない場合は999
    """
    try:
        return PREFECTURE_ORDER.index(prefecture_name)
    except ValueError:
        return 999  # 不明な都道府県は最後


def extract_prefecture_from_address(address: str) -> str:
    """
    住所文字列から都道府県名を抽出

    Args:
        address: 住所文字列

    Returns:
        str: 都道府県名、見つからない場合は空文字
    """
    if not address or not isinstance(address, str):
        return ""

    # 47都道府県のいずれかが含まれているか確認
    for prefecture in PREFECTURE_ORDER:
        if prefecture in address:
            return prefecture

    return ""
