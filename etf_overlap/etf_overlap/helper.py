
import financedatabase as fd
import pandas as pd
from openbb_terminal.sdk import openbb

class EquityHelpers:
    """ Contains helper functions for equity data. """
    
    def get_equity_data(self) -> pd.DataFrame:
        """
        Get dataframe of stock data
        (ticker, name, sector, industry, country)
        """

        orig_eq_json = fd.select_equities()
        new_eq_json = {
            'symbol': [],
            'sector': [],
            'industry': [],
            'country': []
            }

        for symbol in orig_eq_json:
            if symbol != '':
                new_eq_json['symbol'].append(symbol)
                new_eq_json['sector'].append(orig_eq_json[symbol]['sector'])
                new_eq_json['industry'].append(orig_eq_json[symbol]['industry'])
                new_eq_json['country'].append(orig_eq_json[symbol]['country'])
        equity_df = pd.DataFrame.from_dict(new_eq_json)
        return equity_df

    def merge_with_holdings(self, holdings_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge holdings data with equity data generated
        using get_equity_data().
        """
        equity_df = self.get_equity_data()
        merged = holdings_df.merge(
            equity_df,
            how='left',
            on='symbol'
        )
        return merged

class ETFHelpers(EquityHelpers):
    """ Contains helper functions for ETF data. """

    def __init__(self, etf_1: str, etf_2: str):
        self.etf_1 = etf_1
        self.etf_2 = etf_2
        self.merged_etfs = None
        self.greater = self.lesser = None

    def set_merged_etfs(self) -> None:
        """ Merge holdings data into one dataframe. """

        try:
            df_1 = openbb.etf.holdings(self.etf_1)
            df_2 = openbb.etf.holdings(self.etf_2)
            df_1 = self.clean_holdings(df_1)
            df_2 = self.clean_holdings(df_2)

            merged = df_1.merge(
                df_2,
                how='outer',
                on=['symbol', 'name'],
                suffixes=(f'_{self.etf_1}', f'_{self.etf_2}'),
                indicator=True
                )

            self.merged_etfs = merged

        except ValueError:
            raise ValueError(
                """
                ETF data not found for at least one symbol.
                Maybe you misspelled a symbol?
                """)
            
    def set_etf_size(self) -> None:
        """
        Determine which ETF is "lesser" or "greater" based on
        total shares.

        Greater ETF = ETF with higher total shares
        Lesser ETF = ETF with lower total shares
        """
        merged_etfs = self.merged_etfs
        etf_1_total = merged_etfs[f'shares_{self.etf_1}'].sum()
        etf_2_total = merged_etfs[f'shares_{self.etf_2}'].sum()
        self.greater = self.etf_1 if etf_1_total > etf_2_total else self.etf_2
        self.lesser = self.etf_1 if etf_1_total < etf_2_total else self.etf_2
        
    def get_percent_overlap(self, etf: str) -> float:
        """
        Get percentage of overlapping holdings.
        """
        assert etf in (self.etf_1, self.etf_2), 'ETF not in initialized pair'
        merged_etfs = self.merged_etfs
        overlap = merged_etfs[merged_etfs['_merge'] == 'both']
        return round(overlap[f'percent_{etf}'].sum(), 2)

    def get_holdings_overlap(self, etf: str) -> pd.DataFrame:
        """
        Get overlapping holdings based on percentages.
        """
        assert etf in (self.etf_1, self.etf_2), 'ETF not in initialized pair'
        merged_etfs = self.merged_etfs
        overlap = merged_etfs[merged_etfs['_merge'] == 'both']
        overlap = overlap.sort_values(f'percent_{etf}', ascending=False)
        overlap = overlap[['symbol', 'name', f'percent_{etf}', f'shares_{etf}']]
        overlap.columns = ['symbol', 'name', 'percent', 'shares']
        overlap = self.merge_with_holdings(overlap)
        return overlap

    def get_top_overlapping_holdings(self, etf: str, n: int = 10) -> pd.DataFrame:
        """
        Display top n overlapping holdings.
        """
        assert etf in (self.etf_1, self.etf_2), 'ETF not in initialized pair'
        overlap = self.get_holdings_overlap(etf)
        return overlap.head(n)

    @staticmethod
    def clean_holdings(df: pd.DataFrame) -> pd.DataFrame:
        """ Fix dataframe formatting. """
        df = df.reset_index()
        df.columns = ['symbol', 'name', 'percent', 'shares']
        df['percent'] = df['percent'].str.rstrip('%').astype(float) / 100
        return df
