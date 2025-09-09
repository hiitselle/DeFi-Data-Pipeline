
import os
from dotenv import load_dotenv
from defi_pipeline import DeFiDataPipeline

def main():
    # Load environment variables
    load_dotenv()
    
    # Choose your provider (uncomment one)
    # Option 1: Infura
    infura_id = os.getenv('INFURA_PROJECT_ID')
    if infura_id:
        provider_url = f"https://mainnet.infura.io/v3/{infura_id}"
    
    # Option 2: Alchemy  
    # alchemy_key = os.getenv('ALCHEMY_API_KEY')
    # if alchemy_key:
    #     provider_url = f"https://eth-mainnet.alchemyapi.io/v2/{alchemy_key}"
    
    # Option 3: Free public endpoint (rate limited)
    else:
        provider_url = "https://cloudflare-eth.com"
        print("⚠️  Using free public endpoint - consider getting API key for better performance")
    
    try:
        print("🚀 Starting DeFi Data Pipeline...")
        
        # Initialize pipeline
        pipeline = DeFiDataPipeline(provider_url)
        
        # Test connection
        latest_block = pipeline.w3.eth.block_number
        print(f"✅ Connected to Ethereum! Latest block: {latest_block}")
        
        # Run pipeline on last 50 blocks (smaller for testing)
        from_block = latest_block - 50
        to_block = latest_block
        
        print(f"📊 Processing blocks {from_block} to {to_block}")
        pipeline.run_full_pipeline(from_block, to_block)
        
        print("\n🎉 Pipeline completed successfully!")
        print("📁 Check these files:")
        print("   - defi_data.db (SQLite database)")
        print("   - charts/ (visualizations)")
        print("   - exports/ (CSV files)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Troubleshooting:")
        print("   1. Install dependencies: pip install -r requirements.txt")
        print("   2. Add API key to .env file")
        print("   3. Check internet connection")

if __name__ == "__main__":
    main()
