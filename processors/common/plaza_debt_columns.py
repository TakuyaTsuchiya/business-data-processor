"""
プラザ残債更新の列定義

プラザ残債更新処理で使用する列名・列番号を一元管理。
推測や想像を排除し、正確な列名を定義。
"""

class PlazaDebtUpdateColumns:
    """プラザ残債更新の列定義クラス"""
    
    # 入力ファイル1,2: コールセンター回収委託_入金退去情報（Excel）
    COLLECTION_REPORT = {
        'member_no': {
            'name': '会員番号',
            'column': 'D',
            'index': 3  # 0ベース
        },
        'arrears_total': {
            'name': '延滞額合計',
            'column': 'AX',
            'index': 49  # 0ベース
        },
        'report_source': {
            'name': '報告元',
            'column': 'AY',
            'index': 50
        },
        'cancellation_date': {
            'name': '解約申入日',
            'column': 'AZ',
            'index': 51
        },
        'move_out_date': {
            'name': '退去日',
            'column': 'BA',
            'index': 52
        }
    }
    
    # 入力ファイル3: 1241件.csv
    PLAZA_LIST = {
        'management_no': {
            'name': '管理番号',
            'column': 'A',
            'index': 0
        },
        'takeover_no': {
            'name': '引継番号',
            'column': 'B',
            'index': 1
        }
    }
    
    # 出力1: 管理前滞納額情報
    LATE_PAYMENT_OUTPUT_HEADERS = ['管理番号', '管理前滞納額']
    
    # 出力2: 交渉履歴
    NEGOTIATES_OUTPUT_HEADERS = [
        '管理番号', '交渉日時', '担当', '相手', '手段',
        '回収ランク', '結果', '入金予定', '予定金額', '交渉備考'
    ]
    
    # 交渉履歴の固定値
    NEGOTIATES_FIXED_VALUES = {
        '担当': '藤沢陽和',
        '相手': 'その他',
        '手段': 'その他',
        '回収ランク': '',  # 空白
        '結果': 'その他',
        '入金予定': '',  # 空白
        '予定金額': ''   # 空白
    }