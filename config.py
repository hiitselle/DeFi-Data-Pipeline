
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Main configuration class"""
    
    # Web3 Provider Settings
    INFURA_PROJECT_ID = os.getenv('INFURA_PROJECT_ID')
    ALCHEMY_API_KEY = os.getenv('ALCHEMY_API_KEY')
    
    # Database Settings
    DATABASE_PATH = os.getenv('DATABASE_PATH', 'defi_data.db')
    
    # API Rate Limits
    MAX_REQUESTS_PER_MINUTE = int(os.getenv('MAX_REQUESTS_PER_MINUTE', 100))
    BLOCK_BATCH_SIZE = int(os.getenv('BLOCK_BATCH_SIZE', 100))
    
    # Contract Addresses
    UNISWAP_V2_ROUTER = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
    UNISWAP_V2_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
    
    # Event Signatures
    SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
    
    # Output Directories
    CHARTS_DIR = "charts"
    EXPORTS_DIR = "exports"
    
    @classmethod
    def get_web3_provider_url(cls):
        """Get the appropriate Web3 provider URL"""
        if cls.INFURA_PROJECT_ID:
            return f"https://mainnet.infura.io/v3/{cls.INFURA_PROJECT_ID}"
        elif cls.ALCHEMY_API_KEY:
            return f"https://eth-mainnet.alchemyapi.io/v2/{cls.ALCHEMY_API_KEY}"
        else:
            # Fallback to public endpoint
            return "https://cloudflare-eth.com"
    
    @classmethod
    def validate_config(cls):
        """Validate configuration settings"""
        issues = []
        
        if not cls.INFURA_PROJECT_ID and not cls.ALCHEMY_API_KEY:
            issues.append("No API key found. Using public endpoint (rate limited)")
        
        return issues
