"""
Helper utilities for DeFi Data Pipeline
"""

import time
import logging
from typing import Any, Dict, List
from web3 import Web3

logger = logging.getLogger(__name__)

def wei_to_ether(wei_amount: int) -> float:
    """Convert Wei to Ether"""
    return wei_amount / 10**18

def format_address(address: str) -> str:
    """Format Ethereum address for display"""
    if not address:
        return "N/A"
    return f"{address[:6]}...{address[-4:]}"

def calculate_token_amount(raw_amount: int, decimals: int) -> float:
    """Calculate actual token amount from raw amount and decimals"""
    return raw_amount / (10 ** decimals)

def retry_web3_call(func, max_retries: int = 3, delay: float = 1.0):
    """Retry Web3 calls with exponential backoff"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            
            wait_time = delay * (2 ** attempt)
            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)

def validate_ethereum_address(address: str) -> bool:
    """Validate Ethereum address format"""
    try:
        return Web3.is_address(address)
    except:
        return False

def get_block_timestamp_range(w3: Web3, from_block: int, to_block: int) -> tuple:
    """Get timestamp range for a block range"""
    try:
        start_block = w3.eth.get_block(from_block)
        end_block = w3.eth.get_block(to_block)
        return start_block['timestamp'], end_block['timestamp']
    except Exception as e:
        logger.error(f"Error getting block timestamps: {e}")
        return None, None

def estimate_processing_time(block_count: int, avg_events_per_block: int = 10) -> float:
    """Estimate processing time based on block count"""
    # Rough estimate: 0.1 seconds per event
    estimated_events = block_count * avg_events_per_block
    return estimated_events * 0.1

def format_large_number(number: int) -> str:
    """Format large numbers with K, M, B suffixes"""
    if number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.1f}B"
    elif number >= 1_000_000:
        return f"{number / 1_000_000:.1f}M"
    elif number >= 1_000:
        return f"{number / 1_000:.1f}K"
    else:
        return str(number)

def check_web3_connection(w3: Web3) -> bool:
    """Check if Web3 connection is working"""
    try:
        latest_block = w3.eth.block_number
        return latest_block > 0
    except Exception as e:
        logger.error(f"Web3 connection failed: {e}")
        return False

class ProgressTracker:
    """Simple progress tracking for long operations"""
    
    def __init__(self, total: int, description: str = "Processing"):
        self.total = total
        self.current = 0
        self.description = description
        self.start_time = time.time()
    
    def update(self, increment: int = 1):
        """Update progress"""
        self.current += increment
        self._print_progress()
    
    def _print_progress(self):
        """Print progress bar"""
        if self.total == 0:
            return
            
        percentage = (self.current / self.total) * 100
        elapsed = time.time() - self.start_time
        
        if self.current > 0:
            eta = (elapsed / self.current) * (self.total - self.current)
            eta_str = f"ETA: {eta:.0f}s"
        else:
            eta_str = "ETA: --"
        
        bar_length = 40
        filled = int(bar_length * self.current // self.total)
        bar = '█' * filled + '░' * (bar_length - filled)
        
        print(f"\r{self.description}: [{bar}] {percentage:.1f}% ({self.current}/{self.total}) {eta_str}", 
              end='', flush=True)
        
        if self.current >= self.total:
            print()  # New line when complete
