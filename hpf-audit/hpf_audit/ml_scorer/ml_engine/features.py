import sqlite3
import os
from typing import List, Dict, Any

class FeatureExtractor:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.getenv('DB_PATH', 'housing_provident_fund.db')

    def load_data(self) -> List[Dict[str, Any]]:
        """
        Extracts raw data and computes features for all ACTIVE loans.
        Features:
        1. balance_coverage: (Account Balance + Monthly Deposit) / Est. Monthly Installment
        2. overdue_history: Count of past overdue records
        3. deposit_status_abnormal: 1 if not '正常', else 0
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Join Loan (HT), Person (JBXX), and aggregate Repayment (HK) or Overdue (YQ) behavior
        # Note: simplistic estimation for monthly installment (Total / Months)
        sql = """
        SELECT 
            h.DKZH,
            h.GRZH,
            p.XINGMING,
            h.DKJE, 
            h.DKQX,
            p.GRZHYE,
            p.YJCJS,
            p.GRJCZT,
            p.YJCJS,
            p.GRJCZT,
            (SELECT COUNT(*) FROM GR_DK_YQ y WHERE y.DKZH = h.DKZH) as yq_count,
            (SELECT COUNT(*) FROM FX_SJ_JL f WHERE f.ZTID = h.GRZH) as risk_event_count
        FROM GR_DK_HT h
        JOIN GR_JC_JBXX p ON h.GRZH = p.GRZH
        WHERE h.DKZT IN ('已发放', '审批中') 
        """
        
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            features_list = []
            
            for row in rows:
                item = dict(row)
                
                # --- Feature Engineering ---
                
                # 1. Deposit Status Abnormal
                item['feat_deposit_abnormal'] = 1 if item['GRJCZT'] != '正常' else 0
                
                # 2. Balance Coverage Ratio
                # Est. Installment = Loan Amount / Term (Simple linear, ignoring interest for safety)
                # If term is missing, assume 120 months
                term = item['DKQX'] if item['DKQX'] else 120
                if term == 0: term = 1
                est_installment = item['DKJE'] / term
                
                # Money available = Balance + Monthly Deposit
                money_avail = (item['GRZHYE'] or 0) + (item['YJCJS'] or 0)
                
                if est_installment > 0:
                    item['feat_balance_coverage'] = round(money_avail / est_installment, 2)
                else:
                    item['feat_balance_coverage'] = 999 # Safe
                    
                # 3. History
                # 3. History
                item['feat_yq_count'] = item['yq_count']
                
                # 4. Skill Feedback Loop (New)
                # This syncs Skill findings into the ML model
                item['feat_risk_event_count'] = item['risk_event_count']
                
                features_list.append(item)
                
            return features_list
            
        finally:
            conn.close()
