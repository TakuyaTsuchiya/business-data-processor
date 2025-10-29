"""
都道府県順序定数
Business Data Processor

総務省の全国地方公共団体コード（JIS X 0401）に基づく
47都道府県の北から南への順序定義
"""

# 47都道府県の北から南への順序（総務省コード順）
PREFECTURE_ORDER = [
    "北海道",      # 01
    "青森県",      # 02
    "岩手県",      # 03
    "宮城県",      # 04
    "秋田県",      # 05
    "山形県",      # 06
    "福島県",      # 07
    "茨城県",      # 08
    "栃木県",      # 09
    "群馬県",      # 10
    "埼玉県",      # 11
    "千葉県",      # 12
    "東京都",      # 13
    "神奈川県",    # 14
    "新潟県",      # 15
    "富山県",      # 16
    "石川県",      # 17
    "福井県",      # 18
    "山梨県",      # 19
    "長野県",      # 20
    "岐阜県",      # 21
    "静岡県",      # 22
    "愛知県",      # 23
    "三重県",      # 24
    "滋賀県",      # 25
    "京都府",      # 26
    "大阪府",      # 27
    "兵庫県",      # 28
    "奈良県",      # 29
    "和歌山県",    # 30
    "鳥取県",      # 31
    "島根県",      # 32
    "岡山県",      # 33
    "広島県",      # 34
    "山口県",      # 35
    "徳島県",      # 36
    "香川県",      # 37
    "愛媛県",      # 38
    "高知県",      # 39
    "福岡県",      # 40
    "佐賀県",      # 41
    "長崎県",      # 42
    "熊本県",      # 43
    "大分県",      # 44
    "宮崎県",      # 45
    "鹿児島県",    # 46
    "沖縄県",      # 47
]


def get_prefecture_order(prefecture_name: str) -> int:
    """
    都道府県名から順序番号を取得

    Args:
        prefecture_name: 都道府県名（例: "東京都", "北海道"）

    Returns:
        int: 順序番号（0〜46）。見つからない場合は999を返す

    Examples:
        >>> get_prefecture_order("北海道")
        0
        >>> get_prefecture_order("東京都")
        12
        >>> get_prefecture_order("沖縄県")
        46
        >>> get_prefecture_order("不明")
        999
    """
    try:
        return PREFECTURE_ORDER.index(prefecture_name)
    except ValueError:
        # 見つからない場合は最後に配置（大きな番号を返す）
        return 999


def extract_prefecture_from_address(address: str) -> str:
    """
    住所文字列から都道府県を抽出

    Args:
        address: 住所文字列（例: "東京都新宿区", "北海道札幌市"）

    Returns:
        str: 都道府県名。見つからない場合は空文字

    Examples:
        >>> extract_prefecture_from_address("東京都新宿区西新宿")
        "東京都"
        >>> extract_prefecture_from_address("北海道札幌市")
        "北海道"
        >>> extract_prefecture_from_address("不明な住所")
        ""
    """
    if not address:
        return ""

    for prefecture in PREFECTURE_ORDER:
        if address.startswith(prefecture):
            return prefecture

    return ""
