"""
ContractList標準フォーマット列定義

不動産管理システムから出力されるContractListの列番号を定数化。
列順序は固定されているため、列番号でアクセスすることで
列名の表記揺れによるエラーを防ぐ。

使用例:
    from processors.common.contract_list_columns import ContractListColumns as COL
    trustee_id = df.iloc[:, COL.TRUSTEE_ID]
"""


class ContractListColumns:
    """ContractList列定義（0ベースのインデックス）"""
    
    # ========== 基本情報 ==========
    MANAGEMENT_NO = 3         # D列: 管理番号
    CONTRACT_NAME = 7         # H列: 契約者氏名
    CONTRACT_KANA = 8         # I列: 契約者カナ
    PROPERTY_NAME = 15        # P列: 物件名
    PROPERTY_NO = 16          # Q列: 物件番号
    CLIENT_NAME = 115         # DL列: クライアント名
    CLIENT_CD = 116           # DM列: クライアントCD
    
    # ========== 電話番号（対象者別） ==========
    # 契約者
    TEL_MOBILE = 27           # AB列: TEL携帯
    TEL_HOME = 26             # AA列: TEL自宅
    
    # 保証人
    TEL_MOBILE_1 = 40         # AO列: TEL携帯.1
    TEL_HOME_1 = 39           # AN列: TEL自宅.1
    
    # 緊急連絡人
    TEL_MOBILE_2 = 56         # BE列: 緊急連絡人１のTEL（携帯）
    EMERGENCY_CONTACT_NAME = 53  # BB列: 緊急連絡人１の氏名
    
    # ========== 金額・日付関連 ==========
    DEBT_AMOUNT = 71          # BT列: 滞納残債
    PAYMENT_DATE = 72         # BU列: 入金予定日
    PAYMENT_AMOUNT = 73       # BV列: 入金予定金額
    
    # ========== ステータス関連 ==========
    COLLECTION_RANK = 86      # CI列: 回収ランク
    RESIDENCE_STATUS = 108    # DC列: 入居ステータス
    DELINQUENT_STATUS = 109   # DD列: 滞納ステータス
    
    # ========== 委託・回収関連 ==========
    TRUSTEE_ID = 118          # DO列: 委託先法人ID
    
    # ========== 回収口座情報（SMS用） ==========
    # 回収口座は5つの項目から構成される
    COLLECTION_BANK_NAME = 94      # CQ列: 回収口座_金融機関名
    COLLECTION_BRANCH_NAME = 95    # CR列: 回収口座_支店名
    COLLECTION_ACCOUNT_TYPE = 96   # CS列: 回収口座_預金種目
    COLLECTION_ACCOUNT_NO = 97     # CT列: 回収口座_口座番号
    COLLECTION_ACCOUNT_NAME = 98   # CU列: 回収口座_口座名義人