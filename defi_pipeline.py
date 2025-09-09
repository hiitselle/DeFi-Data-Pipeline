import os
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from web3 import Web3
from datetime import datetime, timedelta
import json
import time
import requests
from typing import List, Dict, Any
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DeFiDataPipeline:
    def __init__(self, web3_provider_url: str, db_path: str = "defi_data.db"):
        """
        Initialize the DeFi Data Pipeline
        
        Args:
            web3_provider_url: URL for Web3 provider (Infura, Alchemy, etc.)
            db_path: Path to SQLite database file
        """
        self.w3 = Web3(Web3.HTTPProvider(web3_provider_url))
        self.db_path = db_path
        
        # Uniswap V2 Router and Factory addresses
        self.uniswap_v2_router = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
        self.uniswap_v2_factory = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
        
        # Swap event signature for Uniswap V2
        self.swap_topic = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
        
        # Initialize database
        self.init_database()
        
        logger.info("DeFi Data Pipeline initialized successfully")
    
    def init_database(self):
        """Initialize SQLite database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Trades table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_number INTEGER,
                transaction_hash TEXT,
                pair_address TEXT,
                sender TEXT,
                to_address TEXT,
                amount0_in REAL,
                amount1_in REAL,
                amount0_out REAL,
                amount1_out REAL,
                token0_address TEXT,
                token1_address TEXT,
                timestamp INTEGER,
                gas_price REAL,
                gas_used INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Tokens table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                address TEXT PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                decimals INTEGER,
                total_supply REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Wallets table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                address TEXT PRIMARY KEY,
                total_trades INTEGER DEFAULT 0,
                total_volume_usd REAL DEFAULT 0,
                first_trade_date TIMESTAMP,
                last_trade_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    
    def get_token_info(self, token_address: str) -> Dict[str, Any]:
        """
        Get token information (symbol, name, decimals)
        
        Args:
            token_address: Token contract address
            
        Returns:
            Dictionary with token information
        """
        try:
            # Standard ERC20 ABI for basic token info
            erc20_abi = [
                {
                    "constant": True,
                    "inputs": [],
                    "name": "name",
                    "outputs": [{"name": "", "type": "string"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "symbol",
                    "outputs": [{"name": "", "type": "string"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "decimals",
                    "outputs": [{"name": "", "type": "uint8"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "totalSupply",
                    "outputs": [{"name": "", "type": "uint256"}],
                    "type": "function"
                }
            ]
            
            contract = self.w3.eth.contract(address=token_address, abi=erc20_abi)
            
            return {
                "address": token_address,
                "symbol": contract.functions.symbol().call(),
                "name": contract.functions.name().call(),
                "decimals": contract.functions.decimals().call(),
                "total_supply": contract.functions.totalSupply().call()
            }
        except Exception as e:
            logger.error(f"Error fetching token info for {token_address}: {e}")
            return {
                "address": token_address,
                "symbol": "UNKNOWN",
                "name": "Unknown Token",
                "decimals": 18,
                "total_supply": 0
            }
    
    def fetch_swap_events(self, from_block: int, to_block: int, limit: int = 1000) -> List[Dict]:
        """
        Fetch Uniswap V2 swap events from blockchain
        
        Args:
            from_block: Starting block number
            to_block: Ending block number
            limit: Maximum number of events to fetch
            
        Returns:
            List of swap events
        """
        logger.info(f"Fetching swap events from block {from_block} to {to_block}")
        
        swap_events = []
        
        try:
            # Get swap events
            event_filter = self.w3.eth.filter({
                'fromBlock': from_block,
                'toBlock': to_block,
                'topics': [self.swap_topic]
            })
            
            events = event_filter.get_all_entries()
            
            for event in events[:limit]:
                try:
                    # Decode swap event data
                    block = self.w3.eth.get_block(event['blockNumber'])
                    transaction = self.w3.eth.get_transaction(event['transactionHash'])
                    receipt = self.w3.eth.get_transaction_receipt(event['transactionHash'])
                    
                    # Parse swap data from event topics and data
                    swap_data = {
                        'block_number': event['blockNumber'],
                        'transaction_hash': event['transactionHash'].hex(),
                        'pair_address': event['address'],
                        'sender': '0x' + event['topics'][1].hex()[-40:],
                        'to_address': '0x' + event['topics'][2].hex()[-40:],
                        'amount0_in': int(event['data'][2:66], 16),
                        'amount1_in': int(event['data'][66:130], 16),
                        'amount0_out': int(event['data'][130:194], 16),
                        'amount1_out': int(event['data'][194:258], 16),
                        'timestamp': block['timestamp'],
                        'gas_price': transaction['gasPrice'],
                        'gas_used': receipt['gasUsed']
                    }
                    
                    # Get pair token addresses (simplified - would need pair contract ABI in practice)
                    swap_data['token0_address'] = None
                    swap_data['token1_address'] = None
                    
                    swap_events.append(swap_data)
                    
                except Exception as e:
                    logger.error(f"Error processing event {event['transactionHash'].hex()}: {e}")
                    continue
            
            logger.info(f"Successfully fetched {len(swap_events)} swap events")
            return swap_events
            
        except Exception as e:
            logger.error(f"Error fetching swap events: {e}")
            return []
    
    def save_trades_to_db(self, trades: List[Dict]):
        """Save trades to database"""
        if not trades:
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for trade in trades:
            cursor.execute("""
                INSERT OR IGNORE INTO trades 
                (block_number, transaction_hash, pair_address, sender, to_address,
                 amount0_in, amount1_in, amount0_out, amount1_out, token0_address,
                 token1_address, timestamp, gas_price, gas_used)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade['block_number'], trade['transaction_hash'], trade['pair_address'],
                trade['sender'], trade['to_address'], trade['amount0_in'],
                trade['amount1_in'], trade['amount0_out'], trade['amount1_out'],
                trade['token0_address'], trade['token1_address'], trade['timestamp'],
                trade['gas_price'], trade['gas_used']
            ))
        
        conn.commit()
        conn.close()
        logger.info(f"Saved {len(trades)} trades to database")
    
    def update_wallet_stats(self):
        """Update wallet statistics based on trades"""
        conn = sqlite3.connect(self.db_path)
        
        # Get wallet trade counts and volumes
        query = """
            SELECT sender as wallet,
                   COUNT(*) as trade_count,
                   MIN(datetime(timestamp, 'unixepoch')) as first_trade,
                   MAX(datetime(timestamp, 'unixepoch')) as last_trade
            FROM trades
            GROUP BY sender
        """
        
        wallet_stats = pd.read_sql_query(query, conn)
        
        # Update wallets table
        cursor = conn.cursor()
        for _, row in wallet_stats.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO wallets 
                (address, total_trades, first_trade_date, last_trade_date)
                VALUES (?, ?, ?, ?)
            """, (row['wallet'], row['trade_count'], row['first_trade'], row['last_trade']))
        
        conn.commit()
        conn.close()
        logger.info("Updated wallet statistics")
    
    def get_daily_volume(self, days: int = 30) -> pd.DataFrame:
        """Get daily trading volume statistics"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
            SELECT DATE(datetime(timestamp, 'unixepoch')) as date,
                   COUNT(*) as trade_count,
                   COUNT(DISTINCT sender) as unique_wallets,
                   COUNT(DISTINCT pair_address) as unique_pairs
            FROM trades
            WHERE datetime(timestamp, 'unixepoch') >= datetime('now', '-{} days')
            GROUP BY DATE(datetime(timestamp, 'unixepoch'))
            ORDER BY date
        """.format(days)
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def get_top_tokens(self, limit: int = 10) -> pd.DataFrame:
        """Get top tokens by trade count"""
        conn = sqlite3.connect(self.db_path)
        
        # This is simplified - in practice you'd join with token info
        query = """
            SELECT pair_address,
                   COUNT(*) as trade_count,
                   COUNT(DISTINCT sender) as unique_traders
            FROM trades
            GROUP BY pair_address
            ORDER BY trade_count DESC
            LIMIT {}
        """.format(limit)
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def get_top_wallets(self, limit: int = 10) -> pd.DataFrame:
        """Get top wallets by trade count"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
            SELECT address,
                   total_trades,
                   first_trade_date,
                   last_trade_date
            FROM wallets
            ORDER BY total_trades DESC
            LIMIT {}
        """.format(limit)
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def create_visualizations(self, output_dir: str = "charts"):
        """Create various data visualizations"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Set style
        plt.style.use('default')
        sns.set_palette("husl")
        
        # 1. Daily Volume Chart
        daily_volume = self.get_daily_volume(30)
        if not daily_volume.empty:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
            
            # Trade count over time
            ax1.plot(pd.to_datetime(daily_volume['date']), daily_volume['trade_count'], 
                    marker='o', linewidth=2)
            ax1.set_title('Daily Trade Count (Last 30 Days)')
            ax1.set_ylabel('Number of Trades')
            ax1.grid(True, alpha=0.3)
            
            # Unique wallets over time
            ax2.plot(pd.to_datetime(daily_volume['date']), daily_volume['unique_wallets'], 
                    marker='s', color='orange', linewidth=2)
            ax2.set_title('Daily Unique Wallets')
            ax2.set_xlabel('Date')
            ax2.set_ylabel('Unique Wallets')
            ax2.grid(True, alpha=0.3)
            
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(f'{output_dir}/daily_volume.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        # 2. Top Tokens Chart
        top_tokens = self.get_top_tokens(10)
        if not top_tokens.empty:
            plt.figure(figsize=(12, 6))
            bars = plt.bar(range(len(top_tokens)), top_tokens['trade_count'])
            plt.title('Top Trading Pairs by Volume')
            plt.xlabel('Pair Address (Truncated)')
            plt.ylabel('Trade Count')
            plt.xticks(range(len(top_tokens)), 
                      [addr[:8] + '...' for addr in top_tokens['pair_address']], 
                      rotation=45)
            
            # Add value labels on bars
            for i, bar in enumerate(bars):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                        str(top_tokens.iloc[i]['trade_count']), 
                        ha='center', va='bottom')
            
            plt.grid(True, alpha=0.3, axis='y')
            plt.tight_layout()
            plt.savefig(f'{output_dir}/top_tokens.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        # 3. Top Wallets Chart
        top_wallets = self.get_top_wallets(10)
        if not top_wallets.empty:
            plt.figure(figsize=(12, 6))
            bars = plt.barh(range(len(top_wallets)), top_wallets['total_trades'])
            plt.title('Top Wallets by Trade Count')
            plt.xlabel('Total Trades')
            plt.ylabel('Wallet Address (Truncated)')
            plt.yticks(range(len(top_wallets)), 
                      [addr[:8] + '...' for addr in top_wallets['address']])
            
            # Add value labels
            for i, bar in enumerate(bars):
                plt.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                        str(top_wallets.iloc[i]['total_trades']), 
                        ha='left', va='center')
            
            plt.grid(True, alpha=0.3, axis='x')
            plt.tight_layout()
            plt.savefig(f'{output_dir}/top_wallets.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        logger.info(f"Visualizations saved to {output_dir}/")
    
    def export_to_csv(self, output_dir: str = "exports"):
        """Export data to CSV files"""
        os.makedirs(output_dir, exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        
        # Export trades
        trades_df = pd.read_sql_query("SELECT * FROM trades", conn)
        trades_df.to_csv(f'{output_dir}/trades.csv', index=False)
        
        # Export tokens
        tokens_df = pd.read_sql_query("SELECT * FROM tokens", conn)
        tokens_df.to_csv(f'{output_dir}/tokens.csv', index=False)
        
        # Export wallets
        wallets_df = pd.read_sql_query("SELECT * FROM wallets", conn)
        wallets_df.to_csv(f'{output_dir}/wallets.csv', index=False)
        
        conn.close()
        logger.info(f"Data exported to CSV files in {output_dir}/")
    
    def run_full_pipeline(self, from_block: int = None, to_block: int = None):
        """Run the complete data pipeline"""
        logger.info("Starting full DeFi data pipeline...")
        
        # Set default block range (last 100 blocks if not specified)
        if from_block is None:
            latest_block = self.w3.eth.block_number
            from_block = latest_block - 100
            to_block = latest_block
        
        # Step 1: Extract data
        logger.info("Step 1: Extracting swap events...")
        swap_events = self.fetch_swap_events(from_block, to_block)
        
        # Step 2: Save to database
        logger.info("Step 2: Saving data to database...")
        self.save_trades_to_db(swap_events)
        
        # Step 3: Update analytics
        logger.info("Step 3: Updating wallet statistics...")
        self.update_wallet_stats()
        
        # Step 4: Generate reports
        logger.info("Step 4: Generating analytics reports...")
        daily_vol = self.get_daily_volume()
        top_tokens = self.get_top_tokens()
        top_wallets = self.get_top_wallets()
        
        print("\n=== ANALYTICS SUMMARY ===")
        print(f"Total trades processed: {len(swap_events)}")
        print(f"\nDaily Volume (last 30 days):")
        print(daily_vol.to_string(index=False))
        print(f"\nTop Trading Pairs:")
        print(top_tokens.to_string(index=False))
        print(f"\nTop Wallets:")
        print(top_wallets.to_string(index=False))
        
        # Step 5: Create visualizations
        logger.info("Step 5: Creating visualizations...")
        self.create_visualizations()
        
        # Step 6: Export data
        logger.info("Step 6: Exporting data to CSV...")
        self.export_to_csv()
        
        logger.info("Pipeline completed successfully!")


# Example usage and configuration
if __name__ == "__main__":
    WEB3_PROVIDER_URL = "https://mainnet.infura.io/v3/cae029ba3dce463fb75536b6a4a96186"

    # Alternative: Use a public endpoint 
    # WEB3_PROVIDER_URL = "https://cloudflare-eth.com"
    
    try:
        # Initialize pipeline
        pipeline = DeFiDataPipeline(WEB3_PROVIDER_URL)
        
        # Run the full pipeline
        # This will process the last 100 blocks by default
        pipeline.run_full_pipeline()
        
        print("\n✅ Pipeline completed successfully!")
        print("📁 Check the following directories:")
        print("   - charts/ - for visualizations")
        print("   - exports/ - for CSV exports")
        print("   - defi_data.db - SQLite database")
        
    except Exception as e:
        print(f"❌ Error running pipeline: {e}")
        print("💡 Make sure to:")
        print("   1. Install required packages: pip install web3 pandas matplotlib seaborn")
        print("   2. Set a valid Web3 provider URL")
        print("   3. Check your internet connection")

